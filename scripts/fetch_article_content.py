#!/usr/bin/env python3
"""
fetch_article_content.py (v1)

Purpose
-------
Fetch and extract article content from publisher URLs and store results in a separate table
(public.article_extractions) WITHOUT overwriting public.raw_items.

Inputs
------
DB mode:
- public.raw_items (stores tracking URLs, RSS URLs, etc.)
- public.url_resolutions (maps NewsNow tracking URLs -> final publisher URL)

CSV mode:
- a CSV that contains at least a 'final_url' column (e.g. resolved_v5.csv)

Outputs
-------
DB mode:
- public.article_extractions: one row per raw_item_id with extracted content + metadata + retry state

CSV mode:
- writes an output CSV with extraction results appended.

Recommended extraction engine:
- trafilatura (pip install trafilatura)

Optional fallback:
- Playwright (pip install playwright; playwright install chromium)

Notes
-----
- Respect site terms/robots. Many publishers block automated access.
- For NewsNow items: this script expects resolve_newsnow_links.py to have already populated url_resolutions.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urlparse, parse_qs, unquote

import httpx
import pandas as pd
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore

# psycopg2 only (predictable placeholder behavior)
try:
    import psycopg2
    import psycopg2.extras
except Exception as e:  # pragma: no cover
    psycopg2 = None  # type: ignore

# trafilatura optional (recommended)
try:
    import trafilatura
    from trafilatura.settings import use_config
except Exception:  # pragma: no cover
    trafilatura = None  # type: ignore
    use_config = None  # type: ignore

# Playwright optional
PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except Exception:  # pragma: no cover
    PLAYWRIGHT_AVAILABLE = False


# -----------------------------
# Utilities
# -----------------------------

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

BLOCK_HINTS = [
    "cf-chl-captcha",
    "cloudflare",
    "access denied",
    "verify you are human",
    "captcha",
    "unusual traffic",
    "bot detection",
]

SHARE_HOSTS = {
    "twitter.com",
    "x.com",
    "facebook.com",
    "www.facebook.com",
    "www.linkedin.com",
    "linkedin.com",
    "t.co",
}

BAD_HOSTS = {
    "www.cloudflare.com",
    "cloudflare.com",
    "ad.doubleclick.net",
    "doubleclick.net",
}

SECTION_PATH_HINTS = (
    "/interviews",
    "/category",
    "/tag",
    "/topics",
    "/library",
    "/home",
)

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

def looks_like_block(html: str) -> bool:
    h = (html or "").lower()
    return any(x in h for x in BLOCK_HINTS)

def unwrap_share_url(url: str) -> str:
    """
    If the URL is a social share/intent link that embeds the real article URL, unwrap it.
    """
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        if host in SHARE_HOSTS:
            qs = parse_qs(p.query)
            for key in ("url", "u", "text"):
                if key in qs and qs[key]:
                    candidate = qs[key][0]
                    # sometimes text contains "... https://site/article ..."
                    m = re.search(r"https?://\S+", candidate)
                    if m:
                        candidate = m.group(0)
                    candidate = unquote(candidate)
                    if candidate.startswith("http"):
                        return candidate
        return url
    except Exception:
        return url

def is_bad_candidate(url: str) -> bool:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        if host in BAD_HOSTS:
            return True
        if host in SHARE_HOSTS:
            return True  # will be unwrapped separately if chosen
        # obvious "share" endpoints
        if "intent/tweet" in (p.path or "").lower():
            return True
        if "sharer.php" in (p.path or "").lower():
            return True
        if "share-offsite" in (p.path or "").lower():
            return True
        return False
    except Exception:
        return True

def looks_like_section(url: str) -> bool:
    try:
        p = urlparse(url)
        path = (p.path or "").lower().rstrip("/")
        if not path or path == "/":
            return True
        return any(path.startswith(h) or (h in path and path.endswith(h.strip("/"))) for h in SECTION_PATH_HINTS)
    except Exception:
        return False

def choose_best_url(candidates: List[str], title: Optional[str] = None, domain_hint: Optional[str] = None) -> Optional[str]:
    """
    Pick the best "article-like" URL from candidates.
    We prefer:
      - non-bad hosts
      - domain_hint match (if provided)
      - URLs containing slug-like patterns
    """
    if not candidates:
        return None

    t = (title or "").lower()
    best = None
    best_score = -10_000

    for u in candidates:
        if not u or not u.startswith("http"):
            continue
        u2 = unwrap_share_url(u)
        if is_bad_candidate(u2):
            continue

        try:
            p = urlparse(u2)
            host = (p.netloc or "").lower()
            path = (p.path or "")
        except Exception:
            continue

        score = 0

        if domain_hint and domain_hint.lower().lstrip("www.") in host.lstrip("www."):
            score += 50

        # Prefer longer, slug-like paths
        if len(path) > 20:
            score += 10
        if re.search(r"/\d{4}/\d{2}/\d{2}/", path):
            score += 10
        if re.search(r"/\d{6,}/", path):
            score += 5
        if "-" in path:
            score += 5

        # Penalize obvious section pages
        if looks_like_section(u2):
            score -= 30

        # Title token overlap with URL
        if t:
            tokens = [w for w in re.split(r"[^a-z0-9]+", t) if len(w) >= 5][:8]
            hits = sum(1 for w in tokens if w in u2.lower())
            score += hits * 6

        if score > best_score:
            best_score = score
            best = u2

    return best

def parse_domain_hint(content_snip: Optional[str]) -> Optional[str]:
    """
    Very light heuristic:
    - If content_snip looks like 'Publisher - blah blah', we might map publisher to known domains later.
    For MVP, return None and rely on candidates + DDG.
    """
    return None


# -----------------------------
# DuckDuckGo fallback (HTML)
# -----------------------------

async def ddg_search_best(title: str, domain_hint: Optional[str], client: httpx.AsyncClient) -> Optional[str]:
    """
    Search DuckDuckGo HTML results and return a best guess URL.
    No API key required. This is a best-effort fallback.
    """
    if not title:
        return None

    q = title.strip()
    if domain_hint:
        q = f"site:{domain_hint} {q}"

    url = "https://duckduckgo.com/html/?q=" + quote_plus(q)
    try:
        r = await client.get(url, timeout=20)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "lxml")
        links = []
        for a in soup.select("a.result__a"):
            href = a.get("href")
            if href and href.startswith("http"):
                links.append(href)
        # Sometimes DDG returns redirect links; best effort: prefer domain_hint
        best = choose_best_url(links, title=title, domain_hint=domain_hint)
        return best or (links[0] if links else None)
    except Exception:
        return None


# -----------------------------
# Fetch + extract
# -----------------------------

@dataclass
class ExtractResult:
    status: str
    fetch_url: str
    canonical_url: Optional[str] = None
    title: Optional[str] = None
    author: Optional[str] = None
    site_name: Optional[str] = None
    published_at: Optional[str] = None
    language: Optional[str] = None
    word_count: Optional[int] = None
    content_text: Optional[str] = None
    content_html: Optional[str] = None
    raw_extraction_json: Optional[Dict[str, Any]] = None
    http_status: Optional[int] = None
    content_type: Optional[str] = None
    error: Optional[str] = None
    method: str = "httpx+trafilatura"

def trafilatura_extract(html: str, url: str) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Return (json_dict, text, html_out)
    """
    if trafilatura is None:
        return None, None, None

    # Trafilatura config: be conservative; keep main text
    config = None
    try:
        config = use_config()
        # These keys exist in trafilatura settings; safe even if ignored
        config.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")
    except Exception:
        config = None

    # JSON output includes metadata + text
    try:
        j = trafilatura.extract(
            html,
            url=url,
            output_format="json",
            include_images=False,
            include_links=False,
            include_tables=False,
            include_comments=False,
            favor_precision=True,
            config=config,
        )
        if not j:
            return None, None, None
        jd = json.loads(j)
        text = jd.get("text")
        # Optionally keep cleaned HTML as well
        html_out = trafilatura.extract(
            html,
            url=url,
            output_format="html",
            include_images=False,
            include_links=False,
            include_tables=False,
            include_comments=False,
            favor_precision=True,
            config=config,
        )
        return jd, text, html_out
    except Exception:
        return None, None, None

async def fetch_http(url: str, client: httpx.AsyncClient, max_bytes: int = 3_500_000) -> Tuple[int, str, str]:
    """
    Fetch URL; return (status_code, content_type, html_text)
    """
    r = await client.get(url, timeout=25, follow_redirects=True)
    ct = r.headers.get("content-type", "")
    # Guard max bytes
    b = r.content[:max_bytes]
    try:
        text = b.decode(r.encoding or "utf-8", errors="replace")
    except Exception:
        text = b.decode("utf-8", errors="replace")
    return r.status_code, ct, text

async def extract_with_http(url: str, title: Optional[str], domain_hint: Optional[str],
                            client: httpx.AsyncClient, use_search_fallback: bool) -> ExtractResult:
    try:
        status_code, ct, html = await fetch_http(url, client)
        res = ExtractResult(status="fetch_failed", fetch_url=url, http_status=status_code, content_type=ct)

        if status_code in (401, 403, 429) or looks_like_block(html):
            res.status = "blocked"
            res.error = f"Blocked or forbidden (status={status_code})"
            if use_search_fallback and title:
                best = await ddg_search_best(title, domain_hint, client)
                if best and best != url:
                    # one attempt at fetching the search result
                    status_code2, ct2, html2 = await fetch_http(best, client)
                    if status_code2 == 200 and not looks_like_block(html2):
                        jd, text, html_out = trafilatura_extract(html2, best)
                        if text and len(text.strip()) >= 400:
                            res.status = "ok"
                            res.fetch_url = best
                            res.http_status = status_code2
                            res.content_type = ct2
                            res.raw_extraction_json = jd
                            res.content_text = text.strip()
                            res.content_html = html_out
                            res.method = "ddg+httpx+trafilatura"
                            # best-effort metadata
                            if jd:
                                res.title = jd.get("title") or title
                                res.author = jd.get("author")
                                res.site_name = jd.get("sitename")
                                res.published_at = jd.get("date")
                                res.language = jd.get("language")
                                res.canonical_url = jd.get("url") or best
                                wc = jd.get("word_count") or (len(res.content_text.split()) if res.content_text else None)
                                res.word_count = int(wc) if wc else None
                            return res
            return res

        if status_code != 200:
            res.error = f"HTTP {status_code}"
            return res

        jd, text, html_out = trafilatura_extract(html, url)
        if not text or len(text.strip()) < 300:
            # try to salvage with DDG if we got a section page / weak extraction
            if use_search_fallback and title:
                best = await ddg_search_best(title, domain_hint, client)
                if best and best != url:
                    status_code2, ct2, html2 = await fetch_http(best, client)
                    if status_code2 == 200 and not looks_like_block(html2):
                        jd2, text2, html_out2 = trafilatura_extract(html2, best)
                        if text2 and len(text2.strip()) >= 400:
                            jd, text, html_out, url, status_code, ct = jd2, text2, html_out2, best, status_code2, ct2
                            res.method = "ddg+httpx+trafilatura"

            if not text or len(text.strip()) < 300:
                res.status = "extract_failed"
                res.error = "Extraction produced too little text"
                return res

        res.status = "ok"
        res.raw_extraction_json = jd
        res.content_text = text.strip()
        res.content_html = html_out
        res.http_status = status_code
        res.content_type = ct

        if jd:
            res.title = jd.get("title") or title
            res.author = jd.get("author")
            res.site_name = jd.get("sitename")
            res.published_at = jd.get("date")
            res.language = jd.get("language")
            res.canonical_url = jd.get("url") or url
            wc = jd.get("word_count") or (len(res.content_text.split()) if res.content_text else None)
            res.word_count = int(wc) if wc else None

        return res

    except Exception as e:
        return ExtractResult(status="fetch_failed", fetch_url=url, error=str(e))

async def extract_with_playwright(url: str, title: Optional[str], domain_hint: Optional[str],
                                  profile_dir: Optional[str], use_search_fallback: bool) -> ExtractResult:
    if not PLAYWRIGHT_AVAILABLE:
        return ExtractResult(status="fetch_failed", fetch_url=url, error="Playwright not installed")

    try:
        async with async_playwright() as p:
            if profile_dir:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=profile_dir,
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                page = await context.new_page()
            else:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                page = await context.new_page()

            try:
                await page.set_extra_http_headers({"Accept-Language": "en-GB,en;q=0.9"})
                await page.goto(url, wait_until="domcontentloaded", timeout=35_000)
                await page.wait_for_timeout(2_000)

                html = await page.content()
                final_url = page.url

                jd, text, html_out = trafilatura_extract(html, final_url)
                if text and len(text.strip()) >= 300:
                    res = ExtractResult(status="ok", fetch_url=final_url, content_text=text.strip(), content_html=html_out)
                    res.method = "playwright+trafilatura"
                    res.raw_extraction_json = jd
                    if jd:
                        res.title = jd.get("title") or title
                        res.author = jd.get("author")
                        res.site_name = jd.get("sitename")
                        res.published_at = jd.get("date")
                        res.language = jd.get("language")
                        res.canonical_url = jd.get("url") or final_url
                        wc = jd.get("word_count") or (len(res.content_text.split()) if res.content_text else None)
                        res.word_count = int(wc) if wc else None
                    return res

                # Optional: search fallback even after playwright
                if use_search_fallback and title:
                    async with httpx.AsyncClient(headers=DEFAULT_HEADERS, follow_redirects=True) as client:
                        best = await ddg_search_best(title, domain_hint, client)
                        if best and best != final_url:
                            status_code2, ct2, html2 = await fetch_http(best, client)
                            if status_code2 == 200 and not looks_like_block(html2):
                                jd2, text2, html_out2 = trafilatura_extract(html2, best)
                                if text2 and len(text2.strip()) >= 400:
                                    res = ExtractResult(status="ok", fetch_url=best, content_text=text2.strip(), content_html=html_out2)
                                    res.method = "ddg+httpx+trafilatura"
                                    res.raw_extraction_json = jd2
                                    if jd2:
                                        res.title = jd2.get("title") or title
                                        res.author = jd2.get("author")
                                        res.site_name = jd2.get("sitename")
                                        res.published_at = jd2.get("date")
                                        res.language = jd2.get("language")
                                        res.canonical_url = jd2.get("url") or best
                                        wc = jd2.get("word_count") or (len(res.content_text.split()) if res.content_text else None)
                                        res.word_count = int(wc) if wc else None
                                    return res

                return ExtractResult(status="extract_failed", fetch_url=final_url, error="Playwright extraction too small", method="playwright+trafilatura")

            finally:
                await context.close()

    except Exception as e:
        return ExtractResult(status="fetch_failed", fetch_url=url, error=str(e), method="playwright")


async def extract_one(fetch_url: str, title: Optional[str], content_snip: Optional[str],
                      use_playwright_fallback: bool, profile_dir: Optional[str],
                      delay_min: float, delay_max: float, use_search_fallback: bool) -> ExtractResult:
    # polite delay (jitter)
    if delay_max > 0:
        await asyncio.sleep(random.uniform(delay_min, delay_max))

    domain_hint = parse_domain_hint(content_snip)

    async with httpx.AsyncClient(headers=DEFAULT_HEADERS, follow_redirects=True) as client:
        res = await extract_with_http(fetch_url, title, domain_hint, client, use_search_fallback)
        if res.status == "ok":
            return res

    if use_playwright_fallback:
        return await extract_with_playwright(fetch_url, title, domain_hint, profile_dir, use_search_fallback)

    return res


# -----------------------------
# DB layer
# -----------------------------

DDL_ARTICLE_EXTRACTIONS = """
CREATE TABLE IF NOT EXISTS public.article_extractions (
  raw_item_id uuid PRIMARY KEY REFERENCES public.raw_items(id) ON DELETE CASCADE,
  original_url text NOT NULL,
  resolved_url text NULL,
  fetch_url text NOT NULL,
  canonical_url text NULL,
  title text NULL,
  author text NULL,
  site_name text NULL,
  published_at timestamptz NULL,
  extracted_at timestamptz NOT NULL DEFAULT now(),
  http_status int NULL,
  content_type text NULL,
  language text NULL,
  word_count int NULL,
  content_text text NULL,
  content_html text NULL,
  content_hash text NULL,
  raw_extraction_json jsonb NULL,
  status text NOT NULL DEFAULT 'pending',
  error text NULL,
  attempts int NOT NULL DEFAULT 0,
  next_retry_at timestamptz NULL
);

CREATE INDEX IF NOT EXISTS idx_article_extractions_status_retry
  ON public.article_extractions(status, next_retry_at);

CREATE INDEX IF NOT EXISTS idx_article_extractions_fetch_url
  ON public.article_extractions(fetch_url);
"""

# We avoid literal % in SQL by passing LIKE patterns as parameters.
SQL_FETCH_CANDIDATES = """
WITH candidates AS (
  SELECT
    r.id AS raw_item_id,
    r.url AS original_url,
    CASE
      WHEN r.url LIKE %s THEN u.final_url
      ELSE r.url
    END AS resolved_url,
    CASE
      WHEN r.url LIKE %s THEN u.final_url
      ELSE r.url
    END AS fetch_url,
    r.title AS raw_title,
    r.content_snip AS content_snip,
    r.published_at AS published_at
  FROM public.raw_items r
  LEFT JOIN public.url_resolutions u
    ON u.tracking_url = r.url AND u.status = 'ok'
  WHERE
    (r.url LIKE %s AND u.final_url IS NOT NULL)
    OR
    (r.url NOT LIKE %s)
)
SELECT
  c.raw_item_id, c.original_url, c.resolved_url, c.fetch_url, c.raw_title, c.content_snip, c.published_at,
  a.status AS existing_status, a.next_retry_at AS next_retry_at, a.attempts AS attempts
FROM candidates c
LEFT JOIN public.article_extractions a
  ON a.raw_item_id = c.raw_item_id
WHERE
  a.raw_item_id IS NULL
  OR (
    a.status <> 'ok'
    AND (a.next_retry_at IS NULL OR a.next_retry_at <= now())
  )
ORDER BY c.published_at DESC NULLS LAST
LIMIT %s;
"""

SQL_UPSERT_RESULT = """
INSERT INTO public.article_extractions (
  raw_item_id, original_url, resolved_url, fetch_url, canonical_url,
  title, author, site_name, published_at, extracted_at,
  http_status, content_type, language, word_count,
  content_text, content_html, content_hash, raw_extraction_json,
  status, error, attempts, next_retry_at
) VALUES (
  %s,%s,%s,%s,%s,
  %s,%s,%s,%s, now(),
  %s,%s,%s,%s,
  %s,%s,%s,%s,
  %s,%s,%s,%s
)
ON CONFLICT (raw_item_id) DO UPDATE SET
  resolved_url = EXCLUDED.resolved_url,
  fetch_url = EXCLUDED.fetch_url,
  canonical_url = EXCLUDED.canonical_url,
  title = EXCLUDED.title,
  author = EXCLUDED.author,
  site_name = EXCLUDED.site_name,
  published_at = COALESCE(EXCLUDED.published_at, public.article_extractions.published_at),
  extracted_at = now(),
  http_status = EXCLUDED.http_status,
  content_type = EXCLUDED.content_type,
  language = EXCLUDED.language,
  word_count = EXCLUDED.word_count,
  content_text = EXCLUDED.content_text,
  content_html = EXCLUDED.content_html,
  content_hash = EXCLUDED.content_hash,
  raw_extraction_json = EXCLUDED.raw_extraction_json,
  status = EXCLUDED.status,
  error = EXCLUDED.error,
  attempts = EXCLUDED.attempts,
  next_retry_at = EXCLUDED.next_retry_at;
"""

def compute_next_retry(attempts: int, status: str) -> dt.datetime:
    """
    Exponential backoff w/ caps.
    - blocked: slower retry
    - extract_failed/fetch_failed: faster retry
    """
    attempts = max(1, attempts)
    base_min = 45 if status == "blocked" else 15
    # 2^attempts * base, capped
    minutes = clamp((2 ** attempts) * base_min, 15, 24 * 60)
    jitter = random.randint(-5, 15)
    minutes = clamp(minutes + jitter, 10, 24 * 60)
    return now_utc() + dt.timedelta(minutes=minutes)

def _load_env(dotenv_path: Optional[str]) -> None:
    if dotenv_path and load_dotenv is not None:
        load_dotenv(dotenv_path, override=True)

def _connect_pg(dotenv_path: Optional[str]):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required for --db mode. Install: pip install psycopg2-binary")
    _load_env(dotenv_path)

    # Prefer explicit DATABASE_URL if present and not placeholder junk
    dsn = os.environ.get("DATABASE_URL")
    if dsn and ("USER:PASS@HOST" in dsn or "<DB_USER>" in dsn or "YOUR_USER:YOUR_PASSWORD" in dsn):
        dsn = None

    if not dsn:
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        dbname = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        if not dbname or not user or password is None:
            raise RuntimeError("Missing DB_NAME/DB_USER/DB_PASSWORD in environment or .env")
        # DSN with separate params avoids URL-encoding headaches
        return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)

    return psycopg2.connect(dsn)

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_ARTICLE_EXTRACTIONS)
    conn.commit()

def fetch_candidates(conn, limit: int, only_newsnow: bool) -> List[Dict[str, Any]]:
    like_newsnow = "https://c.newsnow.co.uk/%"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if only_newsnow:
            # Restrict to NewsNow candidates only by adjusting WHERE
            sql = SQL_FETCH_CANDIDATES.replace(
                "(r.url NOT LIKE %s)",
                "FALSE"
            )
            params = (like_newsnow, like_newsnow, like_newsnow, like_newsnow, limit)
            cur.execute(sql, params)
        else:
            params = (like_newsnow, like_newsnow, like_newsnow, like_newsnow, limit)
            cur.execute(SQL_FETCH_CANDIDATES, params)
        rows = cur.fetchall()
        return list(rows)

def upsert_result(conn, row: Dict[str, Any], res: ExtractResult) -> None:
    raw_item_id = row["raw_item_id"]
    original_url = row["original_url"]
    resolved_url = row.get("resolved_url")
    fetch_url = res.fetch_url or row.get("fetch_url")
    canonical_url = res.canonical_url

    attempts_prev = row.get("attempts") or 0
    attempts_new = attempts_prev + 1 if res.status != "ok" else attempts_prev  # don't increment on ok overwrite
    next_retry = None if res.status == "ok" else compute_next_retry(attempts_new, res.status)

    content_hash = None
    if res.content_text:
        content_hash = sha256_text(res.content_text)

    published_at = row.get("published_at")
    # allow trafilatura date string override if parseable
    if res.published_at and not published_at:
        try:
            published_at = dt.datetime.fromisoformat(res.published_at.replace("Z", "+00:00"))
        except Exception:
            pass

    params = (
        raw_item_id, original_url, resolved_url, fetch_url, canonical_url,
        res.title or row.get("raw_title"), res.author, res.site_name, published_at,
        res.http_status, res.content_type, res.language, res.word_count,
        res.content_text, res.content_html, content_hash,
        json.dumps(res.raw_extraction_json) if res.raw_extraction_json else None,
        res.status, res.error, attempts_new, next_retry
    )
    with conn.cursor() as cur:
        cur.execute(SQL_UPSERT_RESULT, params)
    conn.commit()


# -----------------------------
# Runners
# -----------------------------

async def run_batch(rows: List[Dict[str, Any]], args) -> List[Tuple[Dict[str, Any], ExtractResult]]:
    sem = asyncio.Semaphore(args.concurrency)

    async def _one(row: Dict[str, Any]) -> Tuple[Dict[str, Any], ExtractResult]:
        async with sem:
            fetch_url = row.get("fetch_url")
            if not fetch_url or not isinstance(fetch_url, str) or not fetch_url.startswith("http"):
                return row, ExtractResult(status="fetch_failed", fetch_url=str(fetch_url), error="Missing/invalid fetch_url")
            r = await extract_one(
                fetch_url=fetch_url,
                title=row.get("raw_title"),
                content_snip=row.get("content_snip"),
                use_playwright_fallback=args.use_playwright_fallback,
                profile_dir=args.playwright_profile_dir,
                delay_min=args.delay_min,
                delay_max=args.delay_max,
                use_search_fallback=args.search_fallback,
            )
            return row, r

    tasks = [_one(r) for r in rows]
    return await asyncio.gather(*tasks)

def run_db_mode(args) -> int:
    conn = _connect_pg(args.dotenv_path)
    ensure_tables(conn)

    rows = fetch_candidates(conn, args.limit, args.only_newsnow)
    if not rows:
        print("No candidates due for extraction.")
        return 0

    print(f"Fetched {len(rows)} candidates. Extracting with concurrency={args.concurrency}...")

    results = asyncio.run(run_batch(rows, args))

    ok = 0
    for row, res in results:
        upsert_result(conn, row, res)
        if res.status == "ok":
            ok += 1

    print(f"DB extraction complete. OK: {ok}/{len(results)}")
    return 0

def run_csv_mode(args) -> int:
    df = pd.read_csv(args.csv, encoding=args.csv_encoding)
    if "final_url" not in df.columns:
        raise SystemExit("CSV mode expects a 'final_url' column (e.g. resolved_v5.csv).")

    # Only attempt non-empty URLs
    urls = df["final_url"].fillna("").astype(str).tolist()
    rows: List[Dict[str, Any]] = []
    for i, u in enumerate(urls):
        if not u or not u.startswith("http"):
            continue
        title = df["title"].iloc[i] if "title" in df.columns else None
        snip = df["content_snip"].iloc[i] if "content_snip" in df.columns else None
        rows.append({
            "raw_item_id": df["id"].iloc[i] if "id" in df.columns else str(i),
            "original_url": df["url"].iloc[i] if "url" in df.columns else u,
            "resolved_url": u,
            "fetch_url": u,
            "raw_title": title,
            "content_snip": snip,
            "published_at": df["published_at"].iloc[i] if "published_at" in df.columns else None,
            "attempts": 0,
        })

    if args.limit:
        rows = rows[: args.limit]

    if not rows:
        print("No valid URLs to fetch in CSV.")
        df.to_csv(args.out, index=False, encoding="utf-8")
        return 0

    results = asyncio.run(run_batch(rows, args))

    # Build a result frame keyed by raw_item_id
    out_rows = []
    for row, res in results:
        out_rows.append({
            "id": row["raw_item_id"],
            "original_url": row["original_url"],
            "fetch_url": res.fetch_url,
            "status": res.status,
            "error": res.error,
            "title_extracted": res.title,
            "author": res.author,
            "site_name": res.site_name,
            "published_at_extracted": res.published_at,
            "language": res.language,
            "word_count": res.word_count,
            "http_status": res.http_status,
            "content_type": res.content_type,
            "content_hash": sha256_text(res.content_text) if res.content_text else None,
            "content_text": res.content_text,
        })

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out}. Attempted: {len(rows)} | OK: {sum(1 for _, r in results if r.status=='ok')}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch + extract article content into public.article_extractions (separate table).")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--db", action="store_true", help="Run in DB mode (reads raw_items/url_resolutions, writes article_extractions).")
    mode.add_argument("--csv", type=str, help="Run in CSV mode (reads a CSV containing 'final_url').")

    p.add_argument("--out", type=str, default="extracted_articles.csv", help="CSV mode: output file path.")
    p.add_argument("--csv-encoding", type=str, default="utf-8", help="CSV mode: encoding (try latin1/cp1252 if needed).")

    p.add_argument("--limit", type=int, default=150, help="Max number of candidates to process per run.")
    p.add_argument("--concurrency", type=int, default=1, help="Concurrent extractions (keep low; 1-2 recommended).")

    p.add_argument("--delay-min", type=float, default=5, help="Min jitter delay between requests (seconds).")
    p.add_argument("--delay-max", type=float, default=12, help="Max jitter delay between requests (seconds).")

    p.add_argument("--only-newsnow", action="store_true", help="DB mode: only process NewsNow-derived items (requires url_resolutions).")

    p.add_argument("--use-playwright-fallback", action="store_true", help="Use Playwright if HTTP extraction fails/blocked.")
    p.add_argument("--playwright-profile-dir", type=str, default=None, help="Playwright persistent profile directory for cookies/session.")
    p.add_argument("--search-fallback", action="store_true", help="Use DuckDuckGo HTML search fallback when blocked/weak extraction.")

    p.add_argument("--dotenv-path", type=str, default=".env", help="Path to .env (DB mode).")

    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)

    if trafilatura is None:
        print("WARNING: trafilatura is not installed. Install it for better extraction: pip install trafilatura", file=sys.stderr)

    if args.use_playwright_fallback and not PLAYWRIGHT_AVAILABLE:
        print("ERROR: Playwright fallback requested but playwright is not installed.", file=sys.stderr)
        return 2

    if args.db:
        return run_db_mode(args)

    return run_csv_mode(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
