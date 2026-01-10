#!/usr/bin/env python3
"""resolve_newsnow_links.py (v9)

Fixes vs v4:
- Search fallback now actually runs (CSV/DB rows pass title + content_snip into resolver).
- Much less aggressive "article" heuristic (no longer rejects most normal article slugs).
- Better share-link unwrapping (Twitter/X, Facebook, LinkedIn, Nextdoor, mailto, etc.).
- Optional persistent Playwright profile that is reused per-run (not per-URL) for stability.
- DB mode: escaped literal % signs in SQL (LIKE patterns) so psycopg2 placeholder parsing doesn't break.

MVP-first design:
- DB mode supports retry scheduling via next_retry_at + attempts in public.url_resolutions.
- DB mode can load DB_* variables from a .env file (optional) and build DATABASE_URL.

- Prefer extracting publisher URL from NewsNow interstitial HTML (fast).
- Fall back to Playwright for JS/cookie interstitials.
- If still blocked/section/non-article, optionally fall back to DuckDuckGo (no API key).

USAGE (CSV):
  python scripts/resolve_newsnow_links.py --csv raw_items_output.csv --csv-encoding latin1 --out resolved_v5.csv \
    --concurrency 1 --use-playwright-fallback --search-fallback \
    --playwright-profile-dir .\\tmp\\newsnow_profile \
    --delay-min 20 --delay-max 45

USAGE (DB):
  set DATABASE_URL=postgresql://...
  python scripts/resolve_newsnow_links.py --db --limit 300 --concurrency 1 --use-playwright-fallback --search-fallback \
    --playwright-profile-dir .\\tmp\\newsnow_profile

Dependencies:
  pip install httpx beautifulsoup4 lxml tenacity pandas
  pip install psycopg2-binary       # DB mode (recommended)
  pip install playwright             # optional (fallback)
  playwright install chromium
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse, urlunparse, quote_plus

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential_jitter


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

NEWSNOW_HOSTS = {"c.newsnow.co.uk", "www.newsnow.co.uk", "newsnow.co.uk"}

# Hosts that are almost never the real publisher article
BAD_HOST_SUBSTRINGS = [
    "doubleclick.",
    "adservice.",
    "googlesyndication",
    "googleadservices",
    "cloudflare.com",
    "developers.cloudflare.com",
    "facebook.com/sharer",
    "twitter.com/intent",
    "x.com/intent",
    "linkedin.com/share",
    "t.co/",
]

# If a candidate URL's path is exactly one of these (or starts with these), treat as section-ish
SECTION_PATH_PREFIXES = [
    "/library",
    "/interviews",
    "/category",
    "/categories",
    "/tags",
    "/tag",
    "/topics",
    "/topic",
    "/authors",
    "/author",
    "/newsroom",
    "/news",
    "/blog",
    "/blogs",
    "/healthcare",  # e.g. ibtimes section
]

SHARE_HOSTS = {
    "twitter.com", "www.twitter.com", "x.com", "www.x.com",
    "facebook.com", "www.facebook.com",
    "linkedin.com", "www.linkedin.com",
    "nextdoor.co.uk", "www.nextdoor.co.uk",
}

NEWSNOW_PATH_RE = re.compile(r"^/A/\d+")


@dataclass
class Item:
    tracking_url: str
    title: str = ""
    content_snip: str = ""


@dataclass
class ResolveResult:
    tracking_url: str
    final_url: Optional[str]
    status: str  # ok | blocked | parse_failed | http_error | error
    http_status: Optional[int] = None
    error: Optional[str] = None
    method: Optional[str] = None  # html | playwright | ddg



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def compute_next_retry_at(status: str, attempts: int) -> Optional[datetime]:
    """Exponential backoff with jitter. Returns UTC timestamp for next retry, or None if no retry needed."""
    if status == "ok":
        return None

    # base delays (seconds)
    if status in ("blocked", "http_error"):
        base = 45 * 60  # 45 minutes
        cap = 48 * 3600  # 48 hours
    else:
        base = 20 * 60  # 20 minutes for parse_failed/error
        cap = 24 * 3600  # 24 hours

    # attempts starts at 1 for first try
    exp = min(2 ** max(attempts - 1, 0), 16)
    delay = min(base * exp, cap)

    # jitter +/- 20%
    jitter = random.uniform(-0.2, 0.2) * delay
    return utc_now() + timedelta(seconds=delay + jitter)


def build_database_url_from_parts() -> Optional[str]:
    """Build DATABASE_URL from DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD if present."""
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    pwd = os.environ.get("DB_PASSWORD")

    if not (host and name and user and pwd):
        return None

    user_q = quote_plus(user)
    pwd_q = quote_plus(pwd)
    return f"postgresql://{user_q}:{pwd_q}@{host}:{port}/{name}"


def maybe_load_dotenv(dotenv_path: str = ".env") -> None:
    """Best-effort load of a .env file (no hard dependency)."""
    if not dotenv_path:
        return
    if not os.path.exists(dotenv_path):
        return
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(dotenv_path, override=True)
    except Exception:
        return

def is_newsnow_tracking(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    return p.netloc.lower() == "c.newsnow.co.uk" and NEWSNOW_PATH_RE.match(p.path or "") is not None


def _normalize_url(candidate: str, base: str) -> Optional[str]:
    candidate = (candidate or "").strip()
    if not candidate:
        return None
    if candidate.startswith("//"):
        candidate = "https:" + candidate
    elif candidate.startswith("www."):
        candidate = "https://" + candidate
    elif candidate.startswith("/"):
        candidate = urljoin(base, candidate)

    if candidate.lower().startswith("javascript:"):
        return None

    try:
        p = urlparse(candidate)
    except Exception:
        return None

    if p.scheme not in ("http", "https") or not p.netloc:
        return None

    # strip fragment
    p = p._replace(fragment="")
    return urlunparse(p)


def host_is_newsnow(url: str) -> bool:
    try:
        return urlparse(url).netloc.lower() in NEWSNOW_HOSTS
    except Exception:
        return False


def host_is_bad(url: str) -> bool:
    u = url.lower()
    return any(bad in u for bad in BAD_HOST_SUBSTRINGS)


def unwrap_embedded_url(url: str) -> Optional[str]:
    """If url is a share/intent link, unwrap its embedded target URL."""
    try:
        p = urlparse(url)
    except Exception:
        return None

    host = p.netloc.lower()
    if host not in SHARE_HOSTS and "share" not in (p.path or "").lower():
        # also handle nextdoor sharekit, or generic share URLs
        pass

    qs = parse_qs(p.query)

    # Common parameters used by share endpoints
    for key in ("url", "u", "target", "redirect", "dest", "destination", "source", "link"):
        vals = qs.get(key)
        if not vals:
            continue
        for v in vals:
            v = (v or "").strip()
            if not v:
                continue
            v = unquote(v)
            if v.startswith("http://") or v.startswith("https://"):
                return v

    # Sometimes the URL is embedded in the text/body param (e.g. twitter intent)
    for key in ("text", "body", "message"):
        vals = qs.get(key) or []
        for v in vals:
            v = unquote(v or "")
            m = re.search(r"https?://\S+", v)
            if m:
                return m.group(0).rstrip(").,\"'")

    # DuckDuckGo redirect
    if host.endswith("duckduckgo.com") and p.path.startswith("/l/"):
        uddg = qs.get("uddg", [None])[0]
        if uddg:
            return unquote(uddg)

    return None


def looks_like_section(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return True

    path = (p.path or "/").rstrip("/")
    if path in ("", "/"):
        return True

    # Explicit section-ish prefixes
    for pref in SECTION_PATH_PREFIXES:
        if path == pref.rstrip("/"):
            return True
        if path.startswith(pref.rstrip("/") + "/") and pref in ("/category", "/categories", "/tag", "/tags", "/topics", "/topic", "/authors", "/author"):
            # These are almost always taxonomy pages
            return True

    # common index pages
    if path.endswith("/index") or path.endswith("/index.html") or path.endswith("/index.htm"):
        return True

    return False


def looks_like_article(url: str) -> bool:
    """Heuristic: be permissive. Reject only obvious non-articles."""
    if not url:
        return False
    if host_is_newsnow(url) or host_is_bad(url):
        return False

    try:
        p = urlparse(url)
    except Exception:
        return False

    path = (p.path or "/").rstrip("/")
    if path in ("", "/"):
        return False

    # reject pure section/taxonomy pages
    if looks_like_section(url):
        # but allow /news/<slug> pages for many publishers
        if re.match(r"^/news/[^/]+", path) or re.match(r"^/home/[^/]+", path):
            return True
        return False

    last = path.split("/")[-1]
    # If last segment is very short and looks like a category name, reject.
    if len(last) <= 3:
        return False

    # Strong positives
    if any(last.endswith(ext) for ext in (".html", ".htm", ".article", ".aspx", ".php")):
        return True
    if re.search(r"\d{4,}", path):  # ids / years
        return True
    if "-" in last and len(last) >= 10:
        return True
    # multi-segment paths are usually fine
    if path.count("/") >= 2:
        return True

    # fallback: treat as article if it's not obviously a section
    return True


def extract_candidates_from_html(html: str, base_url: str) -> list[str]:
    """Return a list of external candidates discovered in the HTML."""
    soup = BeautifulSoup(html, "lxml")

    candidates: list[str] = []

    # meta refresh
    meta = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
    if meta and meta.get("content"):
        m = re.search(r"url\s*=\s*(.+)$", meta["content"], re.I)
        if m:
            u = _normalize_url(m.group(1).strip(" ' \""), base_url)
            if u:
                candidates.append(u)

    # anchor hrefs
    for a in soup.find_all("a", href=True):
        href = _normalize_url(a.get("href", ""), base_url)
        if not href:
            continue
        candidates.append(href)

    # de-dupe preserve order
    out = []
    seen = set()
    for u in candidates:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def score_candidate(url: str, title: str, content_snip: str) -> float:
    if not url:
        return -1e9
    if host_is_newsnow(url):
        return -1e6
    if host_is_bad(url):
        return -1e5

    # unwrap share links: we don't want to keep the share URL, but we score it lower.
    u_unwrap = unwrap_embedded_url(url)
    if u_unwrap and u_unwrap != url:
        url = u_unwrap

    score = 0.0

    # Prefer non-share, non-tracker
    if unwrap_embedded_url(url):
        score -= 30.0

    # Prefer matching publisher hint (very loose)
    if content_snip:
        sn = content_snip.lower()
        host = urlparse(url).netloc.lower()
        if any(tok in host for tok in re.findall(r"[a-zA-Z]{4,}", sn)[:5]):
            score += 12.0

    # Prefer URLs that look like articles
    if looks_like_article(url):
        score += 50.0
    elif looks_like_section(url):
        score -= 10.0

    # Title words in URL are a strong signal
    if title:
        twords = [w.lower() for w in re.findall(r"[a-zA-Z0-9]{4,}", title)[:12]]
        u = url.lower()
        matches = sum(1 for w in twords if w in u)
        score += matches * 4.0

    # small preference for longer paths (usually articles)
    score += min(len(url), 300) / 60.0
    return score


def pick_best_candidate(candidates: list[str], title: str, content_snip: str) -> Optional[str]:
    # unwrap share links first to enrich candidate pool
    expanded = []
    for u in candidates:
        expanded.append(u)
        uu = unwrap_embedded_url(u)
        if uu and uu != u:
            expanded.append(uu)

    # filter obvious garbage
    filtered = []
    for u in expanded:
        if host_is_bad(u):
            continue
        filtered.append(u)

    # de-dupe
    seen = set()
    uniq = []
    for u in filtered:
        if u not in seen:
            uniq.append(u)
            seen.add(u)

    if not uniq:
        return None

    scored = sorted(((score_candidate(u, title, content_snip), u) for u in uniq), reverse=True, key=lambda x: x[0])
    return scored[0][1]


@retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(initial=1, max=12))
async def fetch_html(client: httpx.AsyncClient, url: str) -> httpx.Response:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }
    return await client.get(url, headers=headers, follow_redirects=True)


async def resolve_via_html(item: Item, max_hops: int, timeout_s: float = 20.0) -> ResolveResult:
    """Follow NewsNow internal hops and extract best external candidate from HTML."""
    if not is_newsnow_tracking(item.tracking_url):
        return ResolveResult(item.tracking_url, None, "error", error="Not a c.newsnow.co.uk tracking URL", method="html")

    url = item.tracking_url
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            for hop in range(max_hops):
                resp = await fetch_html(client, url)

                if resp.status_code in (401, 403, 429):
                    return ResolveResult(item.tracking_url, None, "blocked", http_status=resp.status_code, error="Blocked by NewsNow", method="html")

                html = resp.text or ""
                candidates = extract_candidates_from_html(html, url)

                best = pick_best_candidate(candidates, item.title, item.content_snip)

                if not best:
                    return ResolveResult(item.tracking_url, None, "parse_failed", http_status=resp.status_code, error="No candidates found", method="html")

                # If best is still NewsNow, hop and try again
                if host_is_newsnow(best):
                    url = best
                    continue

                # If best is share link, unwrap
                unwrapped = unwrap_embedded_url(best)
                if unwrapped and not host_is_newsnow(unwrapped):
                    best = unwrapped

                # Accept if it looks like an article; if not, we still return it but mark parse_failed to enable DDG fallback
                if looks_like_article(best):
                    return ResolveResult(item.tracking_url, best, "ok", http_status=resp.status_code, method="html")

                return ResolveResult(item.tracking_url, best, "parse_failed", http_status=resp.status_code,
                                     error=f"Picked non-article/section URL: {best}", method="html")

            return ResolveResult(item.tracking_url, None, "parse_failed", error=f"Exceeded max hops ({max_hops})", method="html")
    except Exception as e:
        return ResolveResult(item.tracking_url, None, "http_error", error=str(e), method="html")


async def ddg_search_best_url(title: str, hint_domain: Optional[str] = None, timeout_s: float = 20.0) -> Optional[str]:
    """DuckDuckGo HTML search. No API key. Returns best candidate URL."""
    if not title:
        return None

    q = f'"{title}"'
    if hint_domain:
        q = q + f" site:{hint_domain}"

    params = {"q": q}
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    url = "https://html.duckduckgo.com/html/"
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        r = await client.post(url, data=params, headers=headers)
        html = r.text or ""

    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a.result__a"):
        href = a.get("href")
        if not href:
            continue
        href = _normalize_url(href, url)
        if not href:
            continue
        # decode ddg redirect
        unwrapped = unwrap_embedded_url(href)
        if unwrapped:
            href = unwrapped
        if host_is_bad(href) or host_is_newsnow(href):
            continue
        links.append(href)

    # Prefer hint domain if present
    if hint_domain:
        for u in links:
            if urlparse(u).netloc.lower().endswith(hint_domain.lower()):
                return u

    return links[0] if links else None


def extract_domain_hint(content_snip: str, fallback_url: Optional[str]) -> Optional[str]:
    """Best-effort domain hint. If fallback_url exists, use its netloc; else none."""
    if fallback_url:
        try:
            return urlparse(fallback_url).netloc
        except Exception:
            return None
    return None


class PlaywrightSession:
    def __init__(self, profile_dir: Optional[str], headless: bool = True):
        self.profile_dir = profile_dir
        self.headless = headless
        self._pw = None
        self._context = None

    async def __aenter__(self):
        from playwright.async_api import async_playwright  # type: ignore

        self._pw = await async_playwright().start()
        chromium = self._pw.chromium

        if self.profile_dir:
            os.makedirs(self.profile_dir, exist_ok=True)
            self._context = await chromium.launch_persistent_context(
                user_data_dir=self.profile_dir,
                headless=self.headless,
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1280, "height": 800},
                locale="en-GB",
            )
        else:
            browser = await chromium.launch(headless=self.headless)
            self._context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1280, "height": 800},
                locale="en-GB",
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self._context:
                await self._context.close()
        finally:
            if self._pw:
                await self._pw.stop()

    async def resolve(self, item: Item, timeout_ms: int = 25000) -> ResolveResult:
        if not is_newsnow_tracking(item.tracking_url):
            return ResolveResult(item.tracking_url, None, "error", error="Not a c.newsnow.co.uk tracking URL", method="playwright")

        try:
            page = await self._context.new_page()
            await page.goto(item.tracking_url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Try clicking "Continue" if present
            try:
                link = page.get_by_role("link", name=re.compile(r"continue to article|click here|continue", re.I))
                if await link.count() > 0:
                    async with page.expect_navigation(wait_until="domcontentloaded", timeout=timeout_ms):
                        await link.first.click()
            except Exception:
                pass

            # Wait for leaving NewsNow
            start = time.time()
            while time.time() - start < 10:
                cur = page.url
                if cur and not host_is_newsnow(cur):
                    final = cur
                    # unwrap share
                    u2 = unwrap_embedded_url(final)
                    if u2 and not host_is_newsnow(u2):
                        final = u2
                    if looks_like_article(final):
                        await page.close()
                        return ResolveResult(item.tracking_url, final, "ok", method="playwright")
                    await page.close()
                    return ResolveResult(item.tracking_url, final, "parse_failed", error=f"Picked non-article/section URL: {final}", method="playwright")
                await page.wait_for_timeout(500)

            # If still on NewsNow, try to extract anchors from DOM and score
            try:
                html = await page.content()
                candidates = extract_candidates_from_html(html, page.url)
                best = pick_best_candidate(candidates, item.title, item.content_snip)
                if best:
                    if host_is_newsnow(best):
                        await page.close()
                        return ResolveResult(item.tracking_url, None, "parse_failed", error="Stayed on NewsNow", method="playwright")
                    u2 = unwrap_embedded_url(best)
                    if u2 and not host_is_newsnow(u2):
                        best = u2
                    if looks_like_article(best):
                        await page.close()
                        return ResolveResult(item.tracking_url, best, "ok", method="playwright")
                    await page.close()
                    return ResolveResult(item.tracking_url, best, "parse_failed", error=f"Picked non-article/section URL: {best}", method="playwright")
            except Exception:
                pass

            await page.close()
            return ResolveResult(item.tracking_url, None, "parse_failed", error="Playwright did not resolve off NewsNow", method="playwright")
        except Exception as e:
            return ResolveResult(item.tracking_url, None, "error", error=str(e), method="playwright")


async def resolve_one(item: Item, args, pw_session: Optional[PlaywrightSession]) -> ResolveResult:
    # 1) HTML resolution (fast)
    r = await resolve_via_html(item, max_hops=args.max_hops)

    # 2) Playwright fallback
    if r.status != "ok" and args.use_playwright_fallback and pw_session is not None:
        r2 = await pw_session.resolve(item)
        if r2.status == "ok":
            r = r2
        else:
            # prefer the better signal (blocked beats parse_failed)
            if r.status == "blocked":
                pass
            else:
                r = r2

    # 3) DuckDuckGo fallback
    if args.search_fallback and r.status != "ok" and item.title:
        hint_domain = extract_domain_hint(item.content_snip, r.final_url)
        try:
            ddg_url = await ddg_search_best_url(item.title, hint_domain=hint_domain)
        except Exception as e:
            ddg_url = None
            r.error = (r.error or "") + f" | DDG error: {e}"

        if ddg_url and looks_like_article(ddg_url):
            return ResolveResult(item.tracking_url, ddg_url, "ok", method="ddg")

        if ddg_url:
            # keep best we found but label parse_failed
            return ResolveResult(item.tracking_url, ddg_url, "parse_failed", error=f"DDG returned non-article URL: {ddg_url}", method="ddg")

    return r


async def resolve_many(items: list[Item], args) -> list[ResolveResult]:
    sem = asyncio.Semaphore(args.concurrency)
    results: list[ResolveResult] = []

    pw_session: Optional[PlaywrightSession] = None
    if args.use_playwright_fallback:
        pw_session = PlaywrightSession(profile_dir=args.playwright_profile_dir, headless=True)
        await pw_session.__aenter__()

    async def _run_one(it: Item):
        async with sem:
            res = await resolve_one(it, args, pw_session)
            results.append(res)
            # pacing between requests (important when concurrency=1)
            if args.delay_max > 0:
                await asyncio.sleep(random.uniform(args.delay_min, args.delay_max))

    try:
        tasks = [asyncio.create_task(_run_one(it)) for it in items]
        await asyncio.gather(*tasks)
    finally:
        if pw_session is not None:
            await pw_session.__aexit__(None, None, None)

    return results


DDL_URL_RESOLUTIONS = """
CREATE TABLE IF NOT EXISTS public.url_resolutions (
  tracking_url     text PRIMARY KEY,
  final_url        text NULL,
  status           text NOT NULL,
  http_status      int NULL,
  error            text NULL,
  method           text NULL,
  attempts         int NOT NULL DEFAULT 0,
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_attempt_at  timestamptz NULL,
  next_retry_at    timestamptz NULL,
  resolved_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_url_resolutions_status ON public.url_resolutions(status);
CREATE INDEX IF NOT EXISTS idx_url_resolutions_next_retry ON public.url_resolutions(next_retry_at);
"""


SQL_FETCH_UNRESOLVED = """
SELECT
  r.url,
  COALESCE(r.title,''),
  COALESCE(r.content_snip,''),
  COALESCE(u.attempts, 0) AS attempts
FROM public.raw_items r
LEFT JOIN public.url_resolutions u
  ON u.tracking_url = r.url
WHERE r.url LIKE 'https://c.newsnow.co.uk/%%'
  AND (
    u.tracking_url IS NULL
    OR (
      u.status <> 'ok'
      AND (u.next_retry_at IS NULL OR u.next_retry_at <= now())
    )
  )
ORDER BY
  CASE WHEN u.tracking_url IS NULL THEN 0 ELSE 1 END,
  r.fetched_at DESC
LIMIT %s;
"""


SQL_UPSERT_RESOLUTION = """
INSERT INTO public.url_resolutions (
  tracking_url, final_url, status, http_status, error, method,
  attempts, last_attempt_at, next_retry_at, resolved_at
)
VALUES (
  %s, %s, %s, %s, %s, %s,
  1, now(), %s, now()
)
ON CONFLICT (tracking_url) DO UPDATE SET
  final_url        = EXCLUDED.final_url,
  status           = EXCLUDED.status,
  http_status      = EXCLUDED.http_status,
  error            = EXCLUDED.error,
  method           = EXCLUDED.method,
  attempts         = public.url_resolutions.attempts + 1,
  last_attempt_at  = now(),
  next_retry_at    = EXCLUDED.next_retry_at,
  resolved_at      = now();
"""


def _dsn_looks_placeholder(dsn: str) -> bool:
    """Detect common placeholder/templated DSNs so we don't accidentally connect to HOST/USER/PASS."""
    if not dsn:
        return True
    d = dsn.strip()
    bad_tokens = [
        "USER:PASS@HOST",
        "postgresql://USER:PASS@HOST",
        "YOUR_USER",
        "YOUR_PASSWORD",
        "<DB_USER>",
        "<DB_PASSWORD>",
        "@HOST:",
        "/DB",
    ]
    if any(tok in d for tok in bad_tokens):
        return True
    # host literally 'HOST'
    if re.search(r"@HOST(?::|/)", d):
        return True
    return False


def _connect_pg(dotenv_path: str = ".env"):
    """
    Connect to Postgres for DB mode.

    Rules:
      - Load .env (override=True) so local config wins (prevents stale env vars like HOST).
      - If DATABASE_URL is set and doesn't look like a placeholder, use it.
      - Otherwise build from DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD.
      - Use psycopg2 (pyformat placeholders) consistently.
    """
    maybe_load_dotenv(dotenv_path)

    dsn_env = os.environ.get("DATABASE_URL")
    dsn = dsn_env if (dsn_env and not _dsn_looks_placeholder(dsn_env)) else None
    if not dsn:
        dsn = build_database_url_from_parts()

    if not dsn:
        raise SystemExit(
            "DB mode requires DATABASE_URL OR DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD "
            "(optionally loadable from .env)."
        )

    import psycopg2  # type: ignore
    return psycopg2.connect(dsn)


def run_db_mode(args: argparse.Namespace) -> int:
    conn = _connect_pg(args.dotenv_path)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(DDL_URL_RESOLUTIONS)

    # Pull both new items (no record yet) and retry-due failures.
    cur.execute(SQL_FETCH_UNRESOLVED, (args.limit,))
    rows = cur.fetchall()

    attempts_map: dict[str, int] = {}
    items: list[Item] = []
    for r in rows:
        tracking_url = r[0]
        title = r[1] or ""
        snip = r[2] or ""
        attempts_map[tracking_url] = int(r[3] or 0)
        items.append(Item(tracking_url=tracking_url, title=title, content_snip=snip))

    if not items:
        print("No unresolved NewsNow tracking URLs found (or not due for retry).")
        return 0

    results = asyncio.run(resolve_many(items, args))

    for r in results:
        prev_attempts = attempts_map.get(r.tracking_url, 0)
        this_attempt = prev_attempts + 1
        next_retry = compute_next_retry_at(r.status, this_attempt)
        cur.execute(SQL_UPSERT_RESOLUTION, (r.tracking_url, r.final_url, r.status, r.http_status, r.error, r.method, next_retry))

    ok = sum(1 for r in results if r.status == "ok")
    blocked = sum(1 for r in results if r.status == "blocked")
    failed = len(results) - ok - blocked
    print(f"Resolved OK: {ok} / {len(results)} (blocked: {blocked}, other_fail: {failed}) — saved into public.url_resolutions")
    return 0


def run_csv_mode(args: argparse.Namespace) -> int:
    import pandas as pd

    # read with fallback encodings
    encodings_to_try = [args.csv_encoding, "utf-8-sig", "cp1252", "latin1"]
    last_err = None
    df = None
    for enc in encodings_to_try:
        try:
            df = pd.read_csv(args.csv, encoding=enc)
            break
        except UnicodeDecodeError as e:
            last_err = e
    if df is None:
        raise last_err  # type: ignore

    if "url" not in df.columns:
        raise SystemExit("CSV must contain a 'url' column")

    # build items from df, but only NewsNow tracking URLs
    items: list[Item] = []
    for _, row in df.iterrows():
        u = str(row.get("url", ""))
        if not is_newsnow_tracking(u):
            continue
        title = str(row.get("title", "") or "")
        snip = str(row.get("content_snip", "") or "")
        items.append(Item(tracking_url=u, title=title, content_snip=snip))

    if args.limit:
        items = items[: args.limit]

    results = asyncio.run(resolve_many(items, args))
    mapping = {r.tracking_url: r for r in results}

    df["final_url"] = df["url"].map(lambda u: mapping.get(str(u)).final_url if mapping.get(str(u)) else None)
    df["resolve_status"] = df["url"].map(lambda u: mapping.get(str(u)).status if mapping.get(str(u)) else None)
    df["resolve_error"] = df["url"].map(lambda u: mapping.get(str(u)).error if mapping.get(str(u)) else None)
    df["resolve_method"] = df["url"].map(lambda u: mapping.get(str(u)).method if mapping.get(str(u)) else None)

    df.to_csv(args.out, index=False)
    ok = sum(1 for r in results if r.status == "ok")
    print(f"Wrote {args.out}. Attempted: {len(results)} | OK: {ok}")
    return 0


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Resolve NewsNow tracking URLs to final publisher URLs.")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--csv", help="CSV file containing at least a 'url' column")
    mode.add_argument("--db", action="store_true", help="Resolve URLs from Postgres raw_items table (uses DATABASE_URL)")

    p.add_argument("--out", default="resolved.csv", help="Output CSV (CSV mode only)")
    p.add_argument("--csv-encoding", default="utf-8", help="CSV encoding hint (CSV mode only)")
    p.add_argument("--limit", type=int, default=0, help="Max items to process (0 = all in file/query)")
    p.add_argument("--concurrency", type=int, default=1, help="Concurrent resolutions (keep 1 for NewsNow)")
    p.add_argument("--max-hops", type=int, default=8, help="Max internal NewsNow hops to follow")
    p.add_argument("--delay-min", type=float, default=0.0, help="Min delay (seconds) between items")
    p.add_argument("--delay-max", type=float, default=0.0, help="Max delay (seconds) between items")
    p.add_argument("--use-playwright-fallback", action="store_true", help="Use Playwright if HTML extraction fails")
    p.add_argument("--playwright-profile-dir", default=None, help="Path to persistent Playwright profile dir (cookies etc.)")
    p.add_argument("--search-fallback", action="store_true", help="Use DuckDuckGo search fallback for failed items")
    p.add_argument("--dotenv-path", default=".env", help="Path to .env file to load in DB mode (optional)")
    return p


def main(argv: list[str]) -> int:
    args = build_argparser().parse_args(argv)
    if args.csv:
        return run_csv_mode(args)
    if args.db:
        return run_db_mode(args)
    raise SystemExit("No mode selected")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
    