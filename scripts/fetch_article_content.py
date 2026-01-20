#!/usr/bin/env python3
"""fetch_article_content.py (v10)

Option B storage model (recommended)
-----------------------------------
Store ONE row per unique *cleaned* article URL ("clean_url") and maintain a
separate mapping table that links each NewsNow tracking_url (or other input)
to that article row.

Tables created (if missing)
---------------------------
1) public.article_pages
   - 1 row per unique clean_url
2) public.article_page_sources
   - 1 row per tracking_url (input) mapping to article_pages.id

Inputs
------
DB mode reads from public.url_resolutions (status='ok') and joins raw_items
for title/content_snip *only for better search fallback*.

CSV mode reads a CSV with at least one of these columns:
  - final_url (preferred) OR url OR resolved_url OR fetch_url
  - tracking_url optional
  - title optional

Key improvements vs v9
----------------------
- URL hygiene: strips common tracking/share params (share=linkedin, utm_*, fbclid, gclid, ito, ...)
- Social-share unwrapping (if a share URL is encountered)
- True de-dupe: fetch/extract once per clean_url; map many inputs to one article row
- More aggressive rejection of non-article destinations for search fallback
- Never overwrites a good (ok) article with a worse result

Typical DB usage
----------------
  python scripts/fetch_article_content.py --db --limit 200 --concurrency 1 \
    --use-playwright-fallback --search-fallback \
    --playwright-profile-dir .\\tmp\\publisher_profile \
    --delay-min 10 --delay-max 25 \
    --min-words 140 --max-attempts 5 \
    --dotenv-path .env

"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse, urlunparse

# Optional imports
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore

try:
    import httpx  # type: ignore
except Exception as e:
    raise SystemExit("Missing dependency 'httpx'. Install with: python -m pip install httpx") from e

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception as e:
    raise SystemExit("Missing dependency 'beautifulsoup4'. Install with: python -m pip install beautifulsoup4 lxml") from e

# trafilatura is optional; we fall back to soup extraction if absent
try:
    import trafilatura  # type: ignore
except Exception:
    trafilatura = None  # type: ignore

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception as e:
    raise SystemExit("Missing dependency 'psycopg2-binary'. Install with: python -m pip install psycopg2-binary") from e

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore


# -----------------------------
# URL hygiene & classification
# -----------------------------

BAD_NETLOCS = {
    "twitter.com", "x.com", "www.twitter.com", "www.x.com",
    "facebook.com", "www.facebook.com",
    "linkedin.com", "www.linkedin.com",
    "t.co",
    "newsnow.co.uk", "www.newsnow.co.uk", "c.newsnow.co.uk",
}

BAD_URL_PATTERNS = [
    r"^https?://html\.duckduckgo\.com/",
    r"^https?://duckduckgo\.com/",
    r"^https?://(www\.)?google\.com/",
    r"/intent/tweet",
    r"/share\?",
    r"/sharing/share",
    r"/oauth",
    r"/login",
    r"/signin",
    r"/register",
    r"/subscribe",
    r"/paywall",
    r"/cdn-cgi/",
    r"/wp-login\.php",
]

NON_ARTICLE_PATH_HINTS = [
    "/privacy",
    "/cookie",
    "/cookies",
    "/terms",
    "/conditions",
    "/account",
    "/signin",
    "/login",
    "/subscribe",
]

LOGIN_WALL_MARKERS = [
    "subscribe",
    "subscription",
    "sign in",
    "log in",
    "register",
    "create account",
    "already a subscriber",
    "start your free trial",
    "to continue reading",
    "activate your subscription",
]

WAF_STRONG_MARKERS = [
    "cloudflare",
    "/cdn-cgi/",
    "cf-error-code",
    "cf-ray",
    "challenge-platform",
    "just a moment",
    "attention required",
    "checking your browser",
    "ddos protection",
    "one more step",
    "access denied",
    "reference #",
    "incident id",
    "incapsula",
    "imperva",
    "datadome",
    "perimeterx",
    "px-captcha",
    "captcha",
    "you have been blocked",
    "unusual traffic",
    "automated requests",
]

WAF_WEAK_MARKERS = [
    "enable javascript",
    "enable javascript and cookies",
    "please enable cookies",
]

BLOCK_HTTP_STATUSES = {401, 403, 405, 408, 418, 429, 451, 500, 502, 503, 504}


TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "dclid", "msclkid",
    "mc_cid", "mc_eid",
    "ito",  # metro / share tracking
    "cmpid", "cmp", "ocid", "icid",
    "ref", "ref_src", "referrer", "source",
    "smid", "spm",
    "share",  # only removed for known values; see logic
}

TRACKING_QUERY_PREFIXES = ("utm_", "__", "ga_")

SHARE_PARAM_VALUES_TO_STRIP = {"linkedin", "twitter", "x", "facebook"}


def normalize_url(url: str) -> str:
    if not url:
        return url
    url = url.strip()
    try:
        p = urlparse(url)
        return urlunparse(p._replace(fragment=""))
    except Exception:
        return url


def is_bad_destination(url: str) -> bool:
    if not url:
        return True
    p = urlparse(url)
    nl = (p.netloc or "").lower()
    if nl in BAD_NETLOCS:
        return True
    low = url.lower()
    for pat in BAD_URL_PATTERNS:
        if re.search(pat, low):
            return True
    if low.endswith(".js") or "/code%20cache/" in low or "/code cache/" in low:
        return True
    return False


def unwrap_social_share(url: str) -> str:
    """If a URL is a social share wrapper, try to unwrap its target."""
    if not url:
        return url
    p = urlparse(url)
    host = (p.netloc or "").lower()
    qs = parse_qs(p.query)

    # LinkedIn share wrappers
    if "linkedin.com" in host and ("url" in qs or "mini" in qs):
        u = (qs.get("url") or [None])[0]
        if u:
            return u

    # X/Twitter intent
    if ("twitter.com" in host or "x.com" in host) and ("url" in qs or "text" in qs):
        u = (qs.get("url") or [None])[0]
        if u:
            return u

    return url


def clean_url(url: str) -> str:
    """Normalize + remove obvious tracking/share params without breaking article IDs."""
    if not url:
        return url
    url = unwrap_social_share(normalize_url(url))
    try:
        p = urlparse(url)
        q = parse_qs(p.query, keep_blank_values=True)

        kept: Dict[str, List[str]] = {}
        for k, vals in q.items():
            kl = k.lower()

            # strip utm_*, ga_*, __* etc
            if kl.startswith(TRACKING_QUERY_PREFIXES):
                continue

            # strip known tracking keys
            if kl in TRACKING_QUERY_KEYS:
                if kl == "share":
                    # remove only if value indicates social sharing
                    v0 = (vals[0] if vals else "").lower()
                    if v0 in SHARE_PARAM_VALUES_TO_STRIP or "linkedin" in v0 or "twitter" in v0:
                        continue
                    kept[k] = vals
                else:
                    continue

            else:
                kept[k] = vals

        new_query = urlencode(kept, doseq=True)
        p2 = p._replace(query=new_query, fragment="")
        return urlunparse(p2)
    except Exception:
        return normalize_url(url)


def base_domain_from_netloc(netloc: str) -> str:
    host = (netloc or "").lower().split(":")[0]
    parts = [p for p in host.split(".") if p]
    if len(parts) <= 2:
        return host
    uk_2l = {"co.uk", "org.uk", "gov.uk", "ac.uk", "nhs.uk", "ltd.uk", "plc.uk"}
    last2 = ".".join(parts[-2:])
    last3 = ".".join(parts[-3:])
    if last3 in uk_2l and len(parts) >= 4:
        return ".".join(parts[-4:])
    return last2


def detect_blocked(http_status: Optional[int], html: str) -> bool:
    h = (html or "")[:120_000].lower()
    score = 0
    if http_status is not None and http_status in BLOCK_HTTP_STATUSES:
        score += 2
    for s in WAF_STRONG_MARKERS:
        if s in h:
            score += 2
    weak_hits = sum(1 for w in WAF_WEAK_MARKERS if w in h)
    if weak_hits >= 2:
        score += 1
    return score >= 2


def detect_login_wall(html: str) -> bool:
    h = (html or "")[:60_000].lower()
    for m in LOGIN_WALL_MARKERS:
        if m in h:
            return True
    if "paywall" in h and ("subscribe" in h or "sign in" in h):
        return True
    return False


def looks_like_non_article(url: str, title: str, text: str) -> bool:
    """Heuristic: reject obvious legal/login pages that sometimes appear in fallback."""
    u = (url or "").lower()
    t = (title or "").lower()
    if any(seg in u for seg in NON_ARTICLE_PATH_HINTS):
        # if content is thin, it's very likely not an article
        if word_count(text) < 80:
            return True
    if t in {"terms & conditions", "terms and conditions", "privacy policy", "cookie policy"}:
        return True
    if "linkedin login" in t or "sign in | linkedin" in t:
        return True
    return False


def word_count(text: str) -> int:
    return len(re.findall(r"\w+", text or ""))


def to_hash(text: str) -> Optional[str]:
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


# -----------------------------
# Extraction
# -----------------------------


def soup_fallback_extract(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for sel in ["script", "style", "nav", "footer", "header", "noscript", "aside"]:
        for tag in soup.select(sel):
            tag.decompose()
    for sel in [
        "article",
        "main",
        "div[itemprop='articleBody']",
        "div.article-body",
        "div#article-body",
        "div.content",
    ]:
        node = soup.select_one(sel)
        if node:
            txt = node.get_text("\n", strip=True)
            if txt:
                return txt
    return soup.get_text("\n", strip=True)


def extract_canonical_url(html: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html, "lxml")
        link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
        if link and link.get("href"):
            href = link["href"].strip()
            if href.startswith("http"):
                return href
    except Exception:
        return None
    return None


def extract_with_trafilatura(html: str, url: str) -> Tuple[str, Dict[str, Any]]:
    if trafilatura is None:
        return "", {}
    downloaded = trafilatura.extract(
        html,
        url=url,
        include_comments=False,
        include_tables=False,
        favor_recall=True,
        output_format="json",
        with_metadata=True,
    )
    if not downloaded:
        return "", {}
    try:
        data = json.loads(downloaded)
        text = (data.get("text") or "").strip()
        meta = {
            "title": data.get("title"),
            "author": data.get("author"),
            "date": data.get("date"),
            "sitename": data.get("sitename"),
            "language": data.get("language"),
            "url": data.get("url") or url,
        }
        return text, meta
    except Exception:
        txt = (trafilatura.extract(html, url=url, output_format="txt") or "").strip()
        return txt, {}


# -----------------------------
# HTTP, Search, Playwright
# -----------------------------


@dataclass
class Settings:
    timeout_s: float = 25.0
    proxy: Optional[str] = None
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    )

    delay_min: float = 0.0
    delay_max: float = 0.0

    use_playwright_fallback: bool = False
    playwright_profile_dir: Optional[str] = None
    playwright_headless: bool = True
    search_fallback: bool = False

    min_words: int = 140
    max_attempts: int = 5
    retry_base_minutes: int = 60
    retry_max_hours: int = 48


def compute_next_retry(status: str, attempts_total: int, settings: Settings) -> Optional[datetime]:
    if status == "ok":
        return None
    if attempts_total >= settings.max_attempts:
        return None
    base = settings.retry_base_minutes
    if status in {"blocked", "login_wall"}:
        base = int(base * 3)
    exp = min(max(attempts_total - 1, 0), 8)
    minutes = min(base * (2**exp), settings.retry_max_hours * 60)
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def build_async_client(settings: Settings, headers: Dict[str, str]) -> httpx.AsyncClient:
    kwargs: Dict[str, Any] = {
        "follow_redirects": True,
        "timeout": settings.timeout_s,
        "headers": headers,
    }
    if settings.proxy:
        try:
            return httpx.AsyncClient(**kwargs, proxy=settings.proxy)
        except TypeError:
            return httpx.AsyncClient(**kwargs, proxies=settings.proxy)
    return httpx.AsyncClient(**kwargs)


async def fetch_http(url: str, settings: Settings) -> Tuple[Optional[int], Optional[str], bytes, str]:
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    async with build_async_client(settings, headers=headers) as client:
        r = await client.get(url)
        ct = r.headers.get("content-type")
        return r.status_code, ct, r.content, str(r.url)


async def ddg_search(query: str, settings: Settings, site: Optional[str] = None) -> List[str]:
    q = query.strip()
    if site:
        q = f"site:{site} {q}"
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(q)}"
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    async with build_async_client(settings, headers=headers) as client:
        r = await client.get(url)
        r.raise_for_status()
        html = r.text
    soup = BeautifulSoup(html, "lxml")
    urls: List[str] = []
    for a in soup.select("a.result__a"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "duckduckgo.com/l/?" in href:
            qs = parse_qs(urlparse(href).query)
            u = (qs.get("uddg") or [None])[0]
            if u:
                urls.append(u)
        else:
            urls.append(href)
    # de-dupe
    seen = set()
    out: List[str] = []
    for u in urls:
        u2 = normalize_url(u)
        if u2 in seen:
            continue
        seen.add(u2)
        out.append(u2)
    return out


async def fetch_with_playwright(url: str, settings: Settings) -> Tuple[Optional[int], Optional[str], str, str]:
    """Returns (status_code, content_type, html, final_url)"""
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as e:
        raise RuntimeError("Playwright not installed. Run: python -m pip install playwright && playwright install chromium") from e

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=settings.playwright_headless)
        context_kwargs: Dict[str, Any] = {}
        if settings.playwright_profile_dir:
            # Persistent context preserves cookies across runs (helps NewsNow-style interstitials)
            await browser.close()
            context = await p.chromium.launch_persistent_context(
                settings.playwright_profile_dir,
                headless=settings.playwright_headless,
                user_agent=settings.user_agent,
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=int(settings.timeout_s * 1000))
                await page.wait_for_load_state("networkidle", timeout=int(settings.timeout_s * 1000))
                html = await page.content()
                ct = None
                status = None
                if resp is not None:
                    status = resp.status
                    ct = resp.headers.get("content-type")
                final_url = page.url
                return status, ct, html, final_url
            finally:
                await context.close()
        else:
            context = await browser.new_context(user_agent=settings.user_agent)
            page = await context.new_page()
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=int(settings.timeout_s * 1000))
                await page.wait_for_load_state("networkidle", timeout=int(settings.timeout_s * 1000))
                html = await page.content()
                ct = None
                status = None
                if resp is not None:
                    status = resp.status
                    ct = resp.headers.get("content-type")
                final_url = page.url
                return status, ct, html, final_url
            finally:
                await context.close()
                await browser.close()


def pick_search_candidate(cands: Sequence[str], desired_base_domain: Optional[str]) -> Optional[str]:
    for u in cands:
        if not u or not u.startswith("http"):
            continue
        u2 = clean_url(u)
        if is_bad_destination(u2):
            continue
        if desired_base_domain:
            nl = urlparse(u2).netloc.lower()
            if desired_base_domain not in nl:
                continue
        # reject obvious non-article endpoints
        low = u2.lower()
        if any(seg in low for seg in NON_ARTICLE_PATH_HINTS):
            continue
        return u2
    return None


# -----------------------------
# Data structures
# -----------------------------


@dataclass
class InputRow:
    tracking_url: str
    final_url: str
    title: str
    content_snip: str
    attempts: int = 0


@dataclass
class PageResult:
    key_url: str
    fetched_url: str
    canonical_url: Optional[str]
    status: str
    http_status: Optional[int]
    content_type: Optional[str]
    title: Optional[str]
    author: Optional[str]
    published_at: Optional[str]
    sitename: Optional[str]
    language: Optional[str]
    content_text: Optional[str]
    content_hash: Optional[str]
    word_count: int
    error: Optional[str]


# -----------------------------
# DB DDL + queries
# -----------------------------


DDL_ARTICLE_PAGES = """
CREATE TABLE IF NOT EXISTS public.article_pages (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  key_url text NOT NULL UNIQUE,
  fetched_url text NULL,
  canonical_url text NULL,
  title text NULL,
  author text NULL,
  published_at timestamptz NULL,
  sitename text NULL,
  language text NULL,
  content_text text NULL,
  content_hash text NULL,
  word_count int NULL,
  status text NOT NULL DEFAULT 'new',
  http_status int NULL,
  content_type text NULL,
  last_fetched_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_article_pages_status ON public.article_pages(status);
CREATE INDEX IF NOT EXISTS idx_article_pages_published_at ON public.article_pages(published_at);
CREATE INDEX IF NOT EXISTS idx_article_pages_content_hash ON public.article_pages(content_hash);
"""


DDL_ARTICLE_PAGE_SOURCES = """
CREATE TABLE IF NOT EXISTS public.article_page_sources (
  tracking_url text PRIMARY KEY,
  final_url text NULL,
  key_url text NULL,
  article_page_id uuid NULL REFERENCES public.article_pages(id) ON DELETE SET NULL,
  status text NOT NULL,
  http_status int NULL,
  error text NULL,
  attempts int NOT NULL DEFAULT 0,
  last_attempt_at timestamptz NULL,
  next_retry_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_article_page_sources_status ON public.article_page_sources(status);
CREATE INDEX IF NOT EXISTS idx_article_page_sources_next_retry ON public.article_page_sources(next_retry_at);
"""


SQL_FETCH_CANDIDATES = """
SELECT
  ur.tracking_url,
  ur.final_url,
  COALESCE(r.title,'') AS title,
  COALESCE(r.content_snip,'') AS content_snip,
  COALESCE(aps.attempts, 0) AS attempts
FROM public.url_resolutions ur
LEFT JOIN public.raw_items r
  ON r.url = ur.tracking_url
LEFT JOIN public.article_page_sources aps
  ON aps.tracking_url = ur.tracking_url
WHERE ur.status = 'ok'
  AND ur.final_url IS NOT NULL
  AND (
    aps.tracking_url IS NULL
    OR (
      aps.status <> 'ok'
      AND (aps.next_retry_at IS NULL OR aps.next_retry_at <= now())
    )
  )
ORDER BY
  CASE WHEN aps.tracking_url IS NULL THEN 0 ELSE 1 END,
  ur.resolved_at DESC
LIMIT %s;
"""


SQL_UPSERT_PAGE = """
INSERT INTO public.article_pages (
  key_url, fetched_url, canonical_url, title, author, published_at,
  sitename, language, content_text, content_hash, word_count,
  status, http_status, content_type, last_fetched_at, updated_at
)
VALUES (
  %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s,
  %s, %s, %s, now(), now()
)
ON CONFLICT (key_url) DO UPDATE SET
  -- Never downgrade a good page with a worse status
  status = CASE
    WHEN public.article_pages.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_pages.status
    ELSE EXCLUDED.status
  END,
  -- Preserve existing good content if new extraction is thin/empty
  content_text = CASE
    WHEN (EXCLUDED.status = 'ok' AND EXCLUDED.content_text IS NOT NULL AND length(EXCLUDED.content_text) > 0)
      THEN EXCLUDED.content_text
    ELSE public.article_pages.content_text
  END,
  content_hash = COALESCE(EXCLUDED.content_hash, public.article_pages.content_hash),
  word_count   = COALESCE(EXCLUDED.word_count, public.article_pages.word_count),
  fetched_url  = COALESCE(EXCLUDED.fetched_url, public.article_pages.fetched_url),
  canonical_url= COALESCE(EXCLUDED.canonical_url, public.article_pages.canonical_url),
  title        = COALESCE(EXCLUDED.title, public.article_pages.title),
  author       = COALESCE(EXCLUDED.author, public.article_pages.author),
  published_at = COALESCE(EXCLUDED.published_at, public.article_pages.published_at),
  sitename     = COALESCE(EXCLUDED.sitename, public.article_pages.sitename),
  language     = COALESCE(EXCLUDED.language, public.article_pages.language),
  http_status  = COALESCE(EXCLUDED.http_status, public.article_pages.http_status),
  content_type = COALESCE(EXCLUDED.content_type, public.article_pages.content_type),
  last_fetched_at = CASE
    WHEN EXCLUDED.status = 'ok' THEN now()
    ELSE public.article_pages.last_fetched_at
  END,
  updated_at = now()
RETURNING id;
"""


SQL_UPSERT_SOURCE = """
INSERT INTO public.article_page_sources (
  tracking_url, final_url, key_url, article_page_id,
  status, http_status, error,
  attempts, last_attempt_at, next_retry_at, updated_at
)
VALUES (
  %s, %s, %s, %s,
  %s, %s, %s,
  1, now(), %s, now()
)
ON CONFLICT (tracking_url) DO UPDATE SET
  final_url = EXCLUDED.final_url,
  key_url = EXCLUDED.key_url,
  article_page_id = COALESCE(EXCLUDED.article_page_id, public.article_page_sources.article_page_id),
  status = EXCLUDED.status,
  http_status = EXCLUDED.http_status,
  error = EXCLUDED.error,
  attempts = public.article_page_sources.attempts + 1,
  last_attempt_at = now(),
  next_retry_at = EXCLUDED.next_retry_at,
  updated_at = now();
"""


# -----------------------------
# Core resolve + extract
# -----------------------------


async def resolve_and_extract_one(key_url: str, title_hint: str, settings: Settings) -> PageResult:
    """Fetch HTML and extract text for a single key_url."""

    key_url = clean_url(key_url)
    if is_bad_destination(key_url):
        return PageResult(
            key_url=key_url,
            fetched_url=key_url,
            canonical_url=None,
            status="not_article",
            http_status=None,
            content_type=None,
            title=None,
            author=None,
            published_at=None,
            sitename=None,
            language=None,
            content_text=None,
            content_hash=None,
            word_count=0,
            error="Bad destination URL",
        )

    # Primary: HTTP
    http_status: Optional[int] = None
    content_type: Optional[str] = None
    fetched_url: str = key_url
    html: Optional[str] = None
    err: Optional[str] = None

    try:
        http_status, content_type, body, final_url = await fetch_http(key_url, settings)
        fetched_url = clean_url(final_url or key_url)
        # decode best-effort
        html = body.decode("utf-8", errors="ignore")
    except Exception as e:
        err = f"HTTP fetch failed: {e}"

    # If HTTP got blocked/login wall or failed, optionally try Playwright
    blocked = False
    login_wall = False
    if html is not None:
        blocked = detect_blocked(http_status, html)
        login_wall = detect_login_wall(html)

    if (err is not None or blocked or login_wall) and settings.use_playwright_fallback:
        try:
            pw_status, pw_ct, pw_html, pw_final = await fetch_with_playwright(key_url, settings)
            http_status = pw_status
            content_type = pw_ct
            html = pw_html
            fetched_url = clean_url(pw_final or key_url)
            err = None
            blocked = detect_blocked(http_status, html)
            login_wall = detect_login_wall(html)
        except Exception as e:
            err = err or f"Playwright failed: {e}"

    # If still blocked/login-wall and search fallback is enabled, try DDG
    if settings.search_fallback and (blocked or login_wall or html is None):
        # Use title hint if available; else last path segment
        q = title_hint.strip() or urlparse(key_url).path.split("/")[-1]
        desired = base_domain_from_netloc(urlparse(key_url).netloc)
        try:
            cands = await ddg_search(q, settings, site=desired)
            cand = pick_search_candidate(cands, desired)
            if cand and cand != key_url:
                # fetch candidate
                http_status, content_type, body, final_url = await fetch_http(cand, settings)
                fetched_url = clean_url(final_url or cand)
                html = body.decode("utf-8", errors="ignore")
                blocked = detect_blocked(http_status, html)
                login_wall = detect_login_wall(html)
        except Exception as e:
            err = err or f"Search fallback failed: {e}"

    if html is None:
        return PageResult(
            key_url=key_url,
            fetched_url=fetched_url,
            canonical_url=None,
            status="fetch_failed",
            http_status=http_status,
            content_type=content_type,
            title=None,
            author=None,
            published_at=None,
            sitename=None,
            language=None,
            content_text=None,
            content_hash=None,
            word_count=0,
            error=err or "No HTML",
        )

    if blocked:
        return PageResult(
            key_url=key_url,
            fetched_url=fetched_url,
            canonical_url=None,
            status="blocked",
            http_status=http_status,
            content_type=content_type,
            title=None,
            author=None,
            published_at=None,
            sitename=None,
            language=None,
            content_text=None,
            content_hash=None,
            word_count=0,
            error="Blocked/WAF",
        )

    if login_wall:
        return PageResult(
            key_url=key_url,
            fetched_url=fetched_url,
            canonical_url=None,
            status="login_wall",
            http_status=http_status,
            content_type=content_type,
            title=None,
            author=None,
            published_at=None,
            sitename=None,
            language=None,
            content_text=None,
            content_hash=None,
            word_count=0,
            error="Login/subscribe wall",
        )

    # Extract canonical
    canonical = extract_canonical_url(html)
    canonical = clean_url(canonical) if canonical else None

    # Extract content
    text = ""
    meta: Dict[str, Any] = {}
    if trafilatura is not None:
        try:
            text, meta = extract_with_trafilatura(html, fetched_url)
        except Exception:
            text, meta = "", {}

    if not text:
        text = soup_fallback_extract(html)

    text = (text or "").strip()
    wc = word_count(text)

    # Title: prefer meta title, else <title>
    title_out = (meta.get("title") if meta else None) or None
    if not title_out:
        try:
            soup = BeautifulSoup(html, "lxml")
            title_tag = soup.find("title")
            if title_tag:
                title_out = (title_tag.get_text(strip=True) or None)
        except Exception:
            pass

    # Reclassify obvious non-article pages
    if looks_like_non_article(fetched_url, title_out or "", text):
        return PageResult(
            key_url=key_url,
            fetched_url=fetched_url,
            canonical_url=canonical,
            status="not_article",
            http_status=http_status,
            content_type=content_type,
            title=title_out,
            author=(meta.get("author") if meta else None),
            published_at=(meta.get("date") if meta else None),
            sitename=(meta.get("sitename") if meta else None),
            language=(meta.get("language") if meta else None),
            content_text=None,
            content_hash=None,
            word_count=wc,
            error="Non-article page",
        )

    if wc < settings.min_words:
        return PageResult(
            key_url=key_url,
            fetched_url=fetched_url,
            canonical_url=canonical,
            status="thin_content",
            http_status=http_status,
            content_type=content_type,
            title=title_out,
            author=(meta.get("author") if meta else None),
            published_at=(meta.get("date") if meta else None),
            sitename=(meta.get("sitename") if meta else None),
            language=(meta.get("language") if meta else None),
            content_text=text,
            content_hash=to_hash(text),
            word_count=wc,
            error=f"Below min_words ({wc}<{settings.min_words})",
        )

    return PageResult(
        key_url=key_url,
        fetched_url=fetched_url,
        canonical_url=canonical,
        status="ok",
        http_status=http_status,
        content_type=content_type,
        title=title_out,
        author=(meta.get("author") if meta else None),
        published_at=(meta.get("date") if meta else None),
        sitename=(meta.get("sitename") if meta else None),
        language=(meta.get("language") if meta else None),
        content_text=text,
        content_hash=to_hash(text),
        word_count=wc,
        error=None,
    )


async def resolve_many(unique_urls: List[Tuple[str, str]], settings: Settings, concurrency: int) -> Dict[str, PageResult]:
    """unique_urls: list of (key_url, title_hint). Returns mapping key_url->PageResult"""
    sem = asyncio.Semaphore(max(1, concurrency))
    results: Dict[str, PageResult] = {}

    async def _run_one(u: str, t: str):
        async with sem:
            res = await resolve_and_extract_one(u, t, settings)
            results[u] = res
            if settings.delay_max > 0:
                await asyncio.sleep(random.uniform(max(0.0, settings.delay_min), max(settings.delay_min, settings.delay_max)))

    tasks = [asyncio.create_task(_run_one(u, t)) for u, t in unique_urls]
    await asyncio.gather(*tasks)
    return results


# -----------------------------
# DB helpers
# -----------------------------


def _load_env(dotenv_path: Optional[str]) -> None:
    if dotenv_path and load_dotenv is not None:
        load_dotenv(dotenv_path, override=False)


def _build_dsn(dotenv_path: Optional[str]) -> str:
    _load_env(dotenv_path)
    # Prefer explicit DB_* settings
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if host and name and user:
        port = port or "5432"
        # psycopg2 supports both DSN string and kwargs; use DSN string
        parts = [f"host={host}", f"port={port}", f"dbname={name}", f"user={user}"]
        if password:
            parts.append(f"password={password}")
        return " ".join(parts)

    # Fallback to DATABASE_URL if it looks real (avoid placeholders)
    dburl = os.getenv("DATABASE_URL")
    if dburl and all(x not in dburl for x in ["USER:PASS", "@HOST", "/DB"]):
        return dburl

    raise RuntimeError("DB connection not configured. Set DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD in .env")


def _connect_pg(dotenv_path: Optional[str]):
    dsn = _build_dsn(dotenv_path)
    return psycopg2.connect(dsn)


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        cur.execute(DDL_ARTICLE_PAGES)
        cur.execute(DDL_ARTICLE_PAGE_SOURCES)
    conn.commit()


# -----------------------------
# Modes
# -----------------------------


def run_db_mode(args) -> int:
    settings = Settings(
        timeout_s=args.timeout,
        proxy=args.proxy,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        use_playwright_fallback=args.use_playwright_fallback,
        playwright_profile_dir=args.playwright_profile_dir,
        playwright_headless=not args.playwright_headful,
        search_fallback=args.search_fallback,
        min_words=args.min_words,
        max_attempts=args.max_attempts,
    )

    # Safety: shared Playwright profile + concurrency>1 is trouble
    if settings.use_playwright_fallback and settings.playwright_profile_dir and args.concurrency > 1:
        print("[warn] Playwright persistent profile dir + concurrency>1 can lock the profile. Forcing concurrency=1.")
        args.concurrency = 1

    conn = _connect_pg(args.dotenv_path)
    ensure_tables(conn)

    # Fetch candidate tracking URLs
    with conn.cursor() as cur:
        cur.execute(SQL_FETCH_CANDIDATES, (args.limit,))
        rows = cur.fetchall()

    inputs: List[InputRow] = []
    for tracking_url, final_url, title, content_snip, attempts in rows:
        inputs.append(InputRow(
            tracking_url=tracking_url,
            final_url=final_url,
            title=title or "",
            content_snip=content_snip or "",
            attempts=int(attempts or 0),
        ))

    if not inputs:
        print("No candidates to fetch.")
        return 0

    # Build unique key_url list
    key_to_title: Dict[str, str] = {}
    tracking_to_key: Dict[str, str] = {}
    for it in inputs:
        key = clean_url(it.final_url)
        tracking_to_key[it.tracking_url] = key
        if key not in key_to_title:
            key_to_title[key] = it.title

    unique_list = [(k, key_to_title.get(k, "")) for k in key_to_title.keys()]

    # Resolve/extract
    results = asyncio.run(resolve_many(unique_list, settings, args.concurrency))

    ok_sources = 0
    attempted_sources = 0

    # Upsert pages, then sources
    with conn.cursor() as cur:
        # Cache page_id by key_url for this run
        page_ids: Dict[str, str] = {}

        for key_url, res in results.items():
            # Store/Upsert page
            # published_at: parse best-effort if ISO; else NULL
            pub = None
            if res.published_at:
                try:
                    # psycopg2 can parse ISO timestamps if passed as string with tz; otherwise just keep None
                    pub = res.published_at
                except Exception:
                    pub = None

            cur.execute(
                SQL_UPSERT_PAGE,
                (
                    res.key_url,
                    res.fetched_url,
                    res.canonical_url,
                    res.title,
                    res.author,
                    pub,
                    res.sitename,
                    res.language,
                    res.content_text,
                    res.content_hash,
                    res.word_count,
                    res.status,
                    res.http_status,
                    res.content_type,
                ),
            )
            page_id = cur.fetchone()[0]
            page_ids[key_url] = str(page_id)

        # Now upsert each tracking_url mapping
        for it in inputs:
            attempted_sources += 1
            key = tracking_to_key[it.tracking_url]
            res = results.get(key)
            if res is None:
                continue

            attempts_total = it.attempts + 1
            next_retry = compute_next_retry(res.status, attempts_total, settings)
            next_retry_val = next_retry.isoformat() if next_retry else None

            cur.execute(
                SQL_UPSERT_SOURCE,
                (
                    it.tracking_url,
                    it.final_url,
                    key,
                    page_ids.get(key),
                    res.status,
                    res.http_status,
                    res.error,
                    next_retry_val,
                ),
            )
            if res.status == "ok":
                ok_sources += 1

    conn.commit()
    conn.close()

    print(f"DB run complete. Attempted inputs: {attempted_sources} | OK mappings: {ok_sources} | Unique URLs fetched: {len(results)}")
    return 0


def run_csv_mode(args) -> int:
    if pd is None:
        raise SystemExit("CSV mode requires pandas. Install with: python -m pip install pandas")

    settings = Settings(
        timeout_s=args.timeout,
        proxy=args.proxy,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        use_playwright_fallback=args.use_playwright_fallback,
        playwright_profile_dir=args.playwright_profile_dir,
        playwright_headless=not args.playwright_headful,
        search_fallback=args.search_fallback,
        min_words=args.min_words,
        max_attempts=args.max_attempts,
    )

    df = pd.read_csv(args.csv, encoding=args.csv_encoding)
    # infer columns
    url_col = None
    for c in ["final_url", "resolved_url", "fetch_url", "url"]:
        if c in df.columns:
            url_col = c
            break
    if not url_col:
        raise SystemExit("CSV must contain one of: final_url, resolved_url, fetch_url, url")

    tracking_col = "tracking_url" if "tracking_url" in df.columns else None
    title_col = "title" if "title" in df.columns else None

    rows: List[InputRow] = []
    for _, r in df.iterrows():
        final_url = str(r.get(url_col) or "").strip()
        if not final_url or not final_url.startswith("http"):
            continue
        tracking_url = str(r.get(tracking_col) or final_url).strip() if tracking_col else final_url
        title = str(r.get(title_col) or "").strip() if title_col else ""
        rows.append(InputRow(tracking_url=tracking_url, final_url=final_url, title=title, content_snip="", attempts=0))

    # De-dupe by key_url
    key_to_title: Dict[str, str] = {}
    tracking_to_key: Dict[str, str] = {}
    for it in rows:
        key = clean_url(it.final_url)
        tracking_to_key[it.tracking_url] = key
        if key not in key_to_title:
            key_to_title[key] = it.title

    unique_list = [(k, key_to_title.get(k, "")) for k in key_to_title.keys()]
    results = asyncio.run(resolve_many(unique_list, settings, args.concurrency))

    out_rows: List[Dict[str, Any]] = []
    for it in rows:
        key = tracking_to_key[it.tracking_url]
        res = results.get(key)
        if not res:
            continue
        out_rows.append({
            "tracking_url": it.tracking_url,
            "final_url": it.final_url,
            "clean_url": key,
            "fetched_url": res.fetched_url,
            "canonical_url": res.canonical_url,
            "resolve_status": res.status,
            "http_status": res.http_status,
            "content_type": res.content_type,
            "title_extracted": res.title,
            "word_count": res.word_count,
            "content_hash": res.content_hash,
            "error": res.error,
        })

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(args.out, index=False)
    print(f"Wrote {args.out}. Inputs: {len(rows)} | Unique fetched: {len(results)} | OK: {sum(1 for r in results.values() if r.status=='ok')}")
    return 0


# -----------------------------
# CLI
# -----------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch & extract article content (Option B: unique clean_url + mapping table)")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--db", action="store_true", help="Run against Postgres (url_resolutions -> article_pages + mapping)")
    mode.add_argument("--csv", type=str, help="Input CSV path")

    p.add_argument("--out", type=str, default="article_extractions.csv", help="CSV output path (CSV mode)")
    p.add_argument("--csv-encoding", type=str, default="utf-8", help="CSV encoding (CSV mode)")

    p.add_argument("--dotenv-path", type=str, default=None, help="Path to .env (DB mode)")
    p.add_argument("--limit", type=int, default=200, help="Max candidates (DB mode)")

    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--timeout", type=float, default=25.0)
    p.add_argument("--proxy", type=str, default=None)

    p.add_argument("--delay-min", type=float, default=0.0)
    p.add_argument("--delay-max", type=float, default=0.0)

    p.add_argument("--use-playwright-fallback", action="store_true")
    p.add_argument("--playwright-profile-dir", type=str, default=None)
    p.add_argument("--playwright-headful", action="store_true", help="Run Playwright headful (debug)")

    p.add_argument("--search-fallback", action="store_true")

    p.add_argument("--min-words", type=int, default=140)
    p.add_argument("--max-attempts", type=int, default=5)
    return p


def main(argv: Sequence[str]) -> int:
    args = build_arg_parser().parse_args(list(argv))
    if args.db:
        return run_db_mode(args)
    return run_csv_mode(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
