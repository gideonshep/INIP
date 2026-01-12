#!/usr/bin/env python3
"""
fetch_article_content.py (v9)

Purpose
- Fetch and extract article text/metadata for URLs we have already resolved via resolve_newsnow_links.py.
- Writes results into public.article_extractions (DB mode) OR outputs a CSV (CSV mode).

Key design choices (MVP-friendly)
- Inputs in DB mode come from public.url_resolutions (status='ok') joined to raw_items for id/title.
  We do NOT scrape arbitrary raw_items URLs directly.
- Never downgrades an 'ok' extraction to a failure on reruns.
- Exponential backoff + max-attempts to avoid hammering paywalled/WAF-protected publishers.
- Optional Playwright fallback and DuckDuckGo search fallback.
- Optional caching by fetch_url within a run (big win when multiple raw_items resolve to the same final_url).

Typical usage (DB mode)
  python scripts/fetch_article_content.py --db --limit 200 --concurrency 1 ^
    --use-playwright-fallback --search-fallback ^
    --playwright-profile-dir .\\tmp\\publisher_profile ^
    --delay-min 10 --delay-max 25 ^
    --min-words 120 --max-attempts 5 ^
    --dotenv-path .env

CSV mode expects at least:
- final_url (preferred) or fetch_url/resolved_url/url columns
- optional title column for search fallback
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote_plus, urlparse, urlunparse

# Optional imports (only used in relevant modes)
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

# trafilatura is the primary extractor
try:
    import trafilatura  # type: ignore
except Exception as e:
    raise SystemExit("Missing dependency 'trafilatura'. Install with: python -m pip install trafilatura") from e

# DB deps
try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore


# -----------------------------
# Detection heuristics
# -----------------------------

BLOCK_HTTP_STATUSES = {401, 403, 405, 408, 418, 429, 451, 500, 502, 503, 504}

WAF_STRONG_MARKERS = [
    # Cloudflare / common WAF & bot challenges
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
    # Akamai / generic "Access Denied" pages
    "akamai",
    "access denied",
    "reference #",
    "incident id",
    # Imperva / Incapsula
    "incapsula",
    "imperva",
    "request unsuccessful",
    # PerimeterX / DataDome
    "perimeterx",
    "px-captcha",
    "datadome",
    # Generic blocks
    "you have been blocked",
    "unusual traffic",
    "automated requests",
    "captcha",
]

# Weak markers are *not* sufficient on their own (many modern sites include these in <noscript>).
WAF_WEAK_MARKERS = [
    "enable javascript",
    "enable javascript and cookies",
    "please enable cookies",
]

# HTTP statuses that are often returned by WAFs / rate limits. (We still try to extract; status alone doesn't doom it.)
BLOCK_HTTP_STATUSES = {401, 403, 405, 429, 451, 503}  # 5xx handled separately
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

BAD_NETLOCS = {
    "twitter.com", "x.com", "www.twitter.com", "www.x.com",
    "facebook.com", "www.facebook.com",
    "linkedin.com", "www.linkedin.com",
    "newsnow.co.uk", "www.newsnow.co.uk", "c.newsnow.co.uk",
    "t.co",
}

BAD_URL_PATTERNS = [
    r"^https?://duckduckgo\.com/",
    r"^https?://(www\.)?google\.com/",
    r"/share\?",  # social share endpoints
    r"/intent/tweet",
    r"/oauth",
    r"/login",
    r"/signin",
    r"/register",
    r"/subscribe",
    r"/paywall",
    r"/cdn-cgi/",
    r"/wp-login\.php",
]

PDF_CT_MARKERS = ("application/pdf", "application/x-pdf")
HTML_CT_MARKERS = ("text/html", "application/xhtml+xml")


def normalize_url(url: str) -> str:
    """Basic normalization: strip fragments, trim whitespace."""
    if not url:
        return url
    url = url.strip()
    try:
        p = urlparse(url)
        # strip fragment; keep query
        p2 = p._replace(fragment="")
        return urlunparse(p2)
    except Exception:
        return url


def looks_like_pdf(url: str, content_type: Optional[str]) -> bool:
    u = (url or "").lower()
    ct = (content_type or "").lower()
    if u.split("?")[0].endswith(".pdf"):
        return True
    return any(m in ct for m in PDF_CT_MARKERS)


def is_bad_destination(url: str) -> bool:
    if not url:
        return True
    p = urlparse(url)
    nl = p.netloc.lower()
    if nl in BAD_NETLOCS:
        return True
    low = url.lower()
    for pat in BAD_URL_PATTERNS:
        if re.search(pat, low):
            return True
    if low.endswith(".js") or "/code%20cache/" in low or "/code cache/" in low:
        return True
    return False


def base_domain_from_netloc(netloc: str) -> str:
    """
    Heuristic base domain for site-restricted search.
    Handles common UK 2nd-level TLDs.
    """
    host = netloc.lower().split(":")[0]
    parts = [p for p in host.split(".") if p]
    if len(parts) <= 2:
        return host
    uk_2l = {"co.uk", "org.uk", "gov.uk", "ac.uk", "nhs.uk", "ltd.uk", "plc.uk"}
    last2 = ".".join(parts[-2:])
    last3 = ".".join(parts[-3:])
    if last3 in uk_2l:
        # e.g. something.bbc.co.uk -> bbc.co.uk (4 parts)
        return ".".join(parts[-4:]) if len(parts) >= 4 else host
    # general case: last2
    return ".".join(parts[-2:])


def detect_blocked(http_status: Optional[int], html: str, *, return_markers: bool = False):
    """Heuristic WAF/challenge detection.

    Key design goal: avoid false positives.
    Many legit sites include 'enable javascript' in <noscript> or cookie banners.
    We only classify as blocked when:
      - status code strongly suggests blocking (401/403/429/503 etc), OR
      - we match at least one STRONG marker, OR
      - we match multiple weak markers together with other signals.

    Returns:
      - bool by default
      - (bool, matched_markers, score) if return_markers=True
    """
    h = (html or "")[:120_000].lower()

    matched: list[str] = []
    score = 0

    if http_status is not None and http_status in BLOCK_HTTP_STATUSES:
        score += 2
        matched.append(f"status:{http_status}")

    for s in WAF_STRONG_MARKERS:
        if s in h:
            score += 2
            matched.append(s)

    weak_hits = 0
    for w in WAF_WEAK_MARKERS:
        if w in h:
            weak_hits += 1
            matched.append(w)

    # Weak markers only count if there are multiple of them, or combined with other signals.
    if weak_hits >= 2:
        score += 1

    blocked = score >= 2  # at least one strong signal
    if return_markers:
        return blocked, matched, score
    return blocked


def detect_login_wall(html: str) -> bool:
    h = (html or "")[:40_000].lower()
    for m in LOGIN_WALL_MARKERS:
        if m in h:
            return True
    # very common "metered paywall" JS markers
    if "paywall" in h and ("subscribe" in h or "sign in" in h):
        return True
    return False


def soup_fallback_extract(html: str) -> str:
    """Last-resort extraction by stripping common chrome/boilerplate."""
    soup = BeautifulSoup(html, "lxml")
    # remove script/style/nav/footer
    for sel in ["script", "style", "nav", "footer", "header", "noscript", "aside"]:
        for tag in soup.select(sel):
            tag.decompose()
    # try common content containers first
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
    txt = soup.get_text("\n", strip=True)
    return txt


def extract_canonical_url(html: str, base_url: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html, "lxml")
        link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
        if link and link.get("href"):
            href = link["href"].strip()
            if href.startswith("http"):
                return href
            # relative canonical
            b = urlparse(base_url)
            return urlunparse(b._replace(path=href))
    except Exception:
        return None
    return None


def extract_with_trafilatura(html: str, url: str) -> Tuple[str, Dict[str, Any]]:
    """
    Returns: (text, meta_dict)
    meta_dict keys may include: title, author, date, sitename, language, url
    """
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
        # fallback to plain text
        txt = (trafilatura.extract(html, url=url, output_format="txt") or "").strip()
        return txt, {}


def pick_search_candidate(cands: Sequence[str], desired_base_domain: Optional[str]) -> Optional[str]:
    for u in cands:
        if not u or not u.startswith("http"):
            continue
        if is_bad_destination(u):
            continue
        if u.lower().split("?")[0].endswith(".pdf"):
            continue
        if desired_base_domain:
            nl = urlparse(u).netloc.lower()
            if desired_base_domain not in nl:
                continue
        return u
    return None


# -----------------------------
# Settings and retry policy
# -----------------------------

@dataclass
class Settings:
    timeout_s: float = 25.0
    proxy: Optional[str] = None
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"

    # pacing
    delay_min: float = 0.0
    delay_max: float = 0.0

    # fallbacks
    use_playwright_fallback: bool = False
    playwright_profile_dir: Optional[str] = None
    playwright_headless: bool = True
    search_fallback: bool = False

    # extraction tuning
    min_words: int = 120
    prefer_html_over_pdf: bool = True
    enable_pdf: bool = False

    # reliability
    cache_by_url: bool = True
    max_attempts: int = 5
    retry_base_minutes: int = 60
    retry_max_hours: int = 48


def sleep_jitter(settings: Settings) -> asyncio.Future:
    if settings.delay_max <= 0:
        return asyncio.sleep(0)
    lo = max(0.0, settings.delay_min)
    hi = max(lo, settings.delay_max)
    return asyncio.sleep(random.uniform(lo, hi))


def compute_next_retry(status: str, attempts_total: int, settings: Settings) -> Optional[datetime]:
    if status == "ok":
        return None
    if attempts_total >= settings.max_attempts:
        return None
    base = settings.retry_base_minutes
    # Make blocked/login wall back off harder
    if status in {"blocked", "login_wall"}:
        base = int(base * 3)
    # Exponential with cap
    exp = min(max(attempts_total - 1, 0), 8)
    minutes = min(base * (2 ** exp), settings.retry_max_hours * 60)
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def word_count(text: str) -> int:
    return len(re.findall(r"\w+", text or ""))


def to_hash(text: str) -> Optional[str]:
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


# -----------------------------
# HTTP + Playwright + Search
# -----------------------------

def build_async_client(settings: Settings, headers: Dict[str, str]) -> httpx.AsyncClient:
    """
    Create an httpx.AsyncClient compatible across httpx versions.

    httpx<=0.27 used `proxies=`; httpx>=0.28 uses `proxy=`.
    """
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


async def fetch_http(url: str, settings: Settings) -> Tuple[Optional[int], Optional[str], bytes]:
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
        return r.status_code, ct, r.content


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
        href = a.get("href")
        if not href:
            continue
        href = href.strip()
        if "duckduckgo.com/l/?" in href:
            qs = parse_qs(urlparse(href).query)
            u = (qs.get("uddg") or [None])[0]
            if u:
                urls.append(u)
        else:
            urls.append(href)
    # de-dupe preserving order
    seen = set()
    out: List[str] = []
    for u in urls:
        u2 = normalize_url(u)
        if u2 in seen:
            continue
        seen.add(u2)
        out.append(u2)
    return out


async def fetch_with_playwright(url: str, settings: Settings) -> Tuple[Optional[int], Optional[str], str]:
    """
    Returns (status_code, content_type, html)
    """
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as e:
        raise RuntimeError("Playwright not installed. Run: python -m pip install playwright && playwright install chromium") from e

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=settings.playwright_headless)
        context = await browser.new_context(
            user_agent=settings.user_agent,
            viewport={"width": 1280, "height": 800},
            locale="en-GB",
            java_script_enabled=True,
        )
        if settings.playwright_profile_dir:
            # Note: Playwright doesn't load Chrome profiles directly; we keep this
            # arg to maintain CLI compatibility and for future enhancements.
            pass

        page = await context.new_page()
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=int(settings.timeout_s * 1000))
            # Give JS redirects / interstitials time to run
            await page.wait_for_load_state("networkidle", timeout=int(settings.timeout_s * 1000))
            html = await page.content()
            status = resp.status if resp else None
            ct = None
            try:
                if resp:
                    ct = (resp.headers or {}).get("content-type")
            except Exception:
                ct = None
            return status, ct, html
        finally:
            await context.close()
            await browser.close()


# -----------------------------
# Extraction pipeline (per URL)
# -----------------------------

async def extract_for_url(
    initial_url: str,
    title_hint: Optional[str],
    settings: Settings,
    desired_base_domain: Optional[str],
) -> Dict[str, Any]:
    """
    Returns dict with keys:
    status, error, fetch_url, canonical_url, http_status, content_type,
    title, author, site_name, published_at, language, content_text, content_html
    """
    fetch_url = normalize_url(initial_url)

    dbg: Dict[str, Any] = {"initial_url": initial_url, "steps": []}

    # If URL is obviously a PDF, prefer searching for HTML if possible
    if settings.prefer_html_over_pdf and title_hint and fetch_url.lower().split("?")[0].endswith(".pdf") and settings.search_fallback:
        try:
            dbg["steps"].append({"step": "search_for_html_instead_of_pdf"})
            site = urlparse(fetch_url).netloc
            q = f'{title_hint} -filetype:pdf'
            cands = await ddg_search(q, settings, site=site or None)
            alt = pick_search_candidate(cands, desired_base_domain or base_domain_from_netloc(site))
            if alt:
                fetch_url = alt
                dbg["steps"].append({"step": "picked_alt_url", "alt": alt})
        except Exception as e:
            dbg["steps"].append({"step": "search_for_html_failed", "error": f"{type(e).__name__}: {e}"})

    # Step 1: HTTP fetch
    try:
        await sleep_jitter(settings)
        http_status, ct, body = await fetch_http(fetch_url, settings)
        dbg["steps"].append({"step": "http_fetch", "status": http_status, "ct": ct, "bytes": len(body)})
    except Exception as e:
        return {
            "status": "fetch_failed",
            "error": f"http_error: {type(e).__name__}: {e}",
            "fetch_url": fetch_url,
            "canonical_url": None,
            "http_status": None,
            "content_type": None,
            "title": None,
            "author": None,
            "site_name": None,
            "published_at": None,
            "language": None,
            "content_text": None,
            "content_html": None,
            "debug": dbg,
        }

    # Step 2: PDF handling (optional)
    if looks_like_pdf(fetch_url, ct):
        if settings.enable_pdf:
            try:
                from pypdf import PdfReader  # type: ignore
                import io
                reader = PdfReader(io.BytesIO(body))
                txt = "\n".join((p.extract_text() or "") for p in reader.pages).strip()
                if txt:
                    return {
                        "status": "ok",
                        "error": None,
                        "fetch_url": fetch_url,
                        "canonical_url": fetch_url,
                        "http_status": http_status,
                        "content_type": ct,
                        "title": title_hint,
                        "author": None,
                        "site_name": None,
                        "published_at": None,
                        "language": None,
                        "content_text": txt,
                        "content_html": None,
                        "debug": dbg,
                    }
                return {
                    "status": "extract_failed",
                    "error": "pdf_extract_empty",
                    "fetch_url": fetch_url,
                    "canonical_url": fetch_url,
                    "http_status": http_status,
                    "content_type": ct,
                    "title": title_hint,
                    "author": None,
                    "site_name": None,
                    "published_at": None,
                    "language": None,
                    "content_text": None,
                    "content_html": None,
                    "debug": dbg,
                }
            except Exception as e:
                return {
                    "status": "extract_failed",
                    "error": f"pdf_extract_failed: {type(e).__name__}: {e}",
                    "fetch_url": fetch_url,
                    "canonical_url": fetch_url,
                    "http_status": http_status,
                    "content_type": ct,
                    "title": title_hint,
                    "author": None,
                    "site_name": None,
                    "published_at": None,
                    "language": None,
                    "content_text": None,
                    "content_html": None,
                    "debug": dbg,
                }

        return {
            "status": "non_html",
            "error": "pdf_detected",
            "fetch_url": fetch_url,
            "canonical_url": fetch_url,
            "http_status": http_status,
            "content_type": ct,
            "title": title_hint,
            "author": None,
            "site_name": None,
            "published_at": None,
            "language": None,
            "content_text": None,
            "content_html": None,
            "debug": dbg,
        }

    # decode HTML
    try:
        html = body.decode("utf-8", errors="replace")
    except Exception:
        html = str(body)

    blocked = detect_blocked(http_status, html)
    login_wall = detect_login_wall(html)

    # Step 3: Playwright fallback if blocked or suspiciously empty
    if settings.use_playwright_fallback and (blocked or len(html) < 2000):
        try:
            dbg["steps"].append({"step": "playwright_fallback_attempt"})
            await sleep_jitter(settings)
            pw_status, pw_ct, pw_html = await fetch_with_playwright(fetch_url, settings)
            if pw_html and len(pw_html) > len(html):
                html = pw_html
                http_status = pw_status or http_status
                ct = pw_ct or ct
                blocked = detect_blocked(http_status, html)
                login_wall = detect_login_wall(html)
                dbg["steps"].append({"step": "playwright_fallback_used", "pw_status": pw_status, "pw_ct": pw_ct})
        except Exception as e:
            dbg["steps"].append({"step": "playwright_fallback_failed", "error": f"{type(e).__name__}: {e}"})

    # Step 4: Search fallback if blocked / empty AND we have a title
    if settings.search_fallback and title_hint and (blocked or len(html) < 2000):
        try:
            dbg["steps"].append({"step": "search_fallback_attempt"})
            site = desired_base_domain or base_domain_from_netloc(urlparse(fetch_url).netloc)
            q = f'{title_hint} -filetype:pdf'
            cands = await ddg_search(q, settings, site=site)
            alt = pick_search_candidate(cands, site)
            if alt and alt != fetch_url:
                dbg["steps"].append({"step": "search_fallback_picked", "alt": alt})
                fetch_url = alt
                await sleep_jitter(settings)
                http_status, ct, body = await fetch_http(fetch_url, settings)
                html = body.decode("utf-8", errors="replace")
                blocked = detect_blocked(http_status, html)
                login_wall = detect_login_wall(html)
        except Exception as e:
            dbg["steps"].append({"step": "search_fallback_failed", "error": f"{type(e).__name__}: {e}"})

    # Don't bail out early on 'blocked' heuristics: attempt extraction first.
    # We'll classify as blocked only if we *also* fail to get meaningful text.
    suspected_blocked = blocked

    # Step 5: Extract text
    canonical = extract_canonical_url(html, fetch_url)
    canonical_norm = normalize_url(canonical) if canonical else None

    text, meta = extract_with_trafilatura(html, fetch_url)
    if not text:
        text = soup_fallback_extract(html)

    # Step 6: If missing or too thin, try canonical / playwright / search
    def _is_good(txt: str) -> bool:
        return word_count(txt) >= settings.min_words

    if canonical_norm and canonical_norm != fetch_url and (not text or not _is_good(text)):
        try:
            dbg["steps"].append({"step": "canonical_retry", "canonical": canonical_norm})
            await sleep_jitter(settings)
            http_status2, ct2, body2 = await fetch_http(canonical_norm, settings)
            if ct2 and any(m in (ct2 or "").lower() for m in HTML_CT_MARKERS):
                html2 = body2.decode("utf-8", errors="replace")
                if not detect_blocked(http_status2, html2):
                    t2, m2 = extract_with_trafilatura(html2, canonical_norm)
                    if not t2:
                        t2 = soup_fallback_extract(html2)
                    if t2 and (not text or word_count(t2) > word_count(text)):
                        text, meta = t2, (m2 or meta)
                        fetch_url = canonical_norm
                        http_status, ct, html = http_status2, ct2, html2
        except Exception as e:
            dbg["steps"].append({"step": "canonical_retry_failed", "error": f"{type(e).__name__}: {e}"})

    if settings.use_playwright_fallback and (not text or not _is_good(text)):
        try:
            dbg["steps"].append({"step": "playwright_retry_for_thin"})
            await sleep_jitter(settings)
            pw_status, pw_ct, pw_html = await fetch_with_playwright(fetch_url, settings)
            if pw_html and not detect_blocked(pw_status, pw_html):
                t3, m3 = extract_with_trafilatura(pw_html, fetch_url)
                if not t3:
                    t3 = soup_fallback_extract(pw_html)
                if t3 and (not text or word_count(t3) > word_count(text)):
                    text, meta = t3, (m3 or meta)
                    http_status = pw_status or http_status
                    ct = pw_ct or ct
        except Exception as e:
            dbg["steps"].append({"step": "playwright_retry_failed", "error": f"{type(e).__name__}: {e}"})

    if settings.search_fallback and title_hint and (not text or not _is_good(text)):
        try:
            dbg["steps"].append({"step": "search_retry_for_thin"})
            site = desired_base_domain or base_domain_from_netloc(urlparse(fetch_url).netloc)
            q = f'{title_hint} -filetype:pdf'
            cands = await ddg_search(q, settings, site=site)
            alt = pick_search_candidate(cands, site)
            if alt and alt != fetch_url:
                dbg["steps"].append({"step": "search_retry_picked", "alt": alt})
                await sleep_jitter(settings)
                http_status4, ct4, body4 = await fetch_http(alt, settings)
                html4 = body4.decode("utf-8", errors="replace")
                if not detect_blocked(http_status4, html4):
                    t4, m4 = extract_with_trafilatura(html4, alt)
                    if not t4:
                        t4 = soup_fallback_extract(html4)
                    if t4 and (not text or word_count(t4) > word_count(text)):
                        text, meta = t4, (m4 or meta)
                        fetch_url = alt
                        http_status, ct = http_status4, ct4
        except Exception as e:
            dbg["steps"].append({"step": "search_retry_failed", "error": f"{type(e).__name__}: {e}"})

    if not text:
        return {
            "status": ("login_wall" if login_wall else ("blocked" if suspected_blocked else "extract_failed")),
            "error": ("login_wall_no_text" if login_wall else (f"blocked_or_waf_no_text (status={http_status})" if suspected_blocked else "no_text")),
            "fetch_url": fetch_url,
            "canonical_url": canonical_norm,
            "http_status": http_status,
            "content_type": ct,
            "title": None,
            "author": None,
            "site_name": None,
            "published_at": None,
            "language": None,
            "content_text": None,
            "content_html": None,
            "debug": dbg,
        }

    wc = word_count(text)
    if wc < settings.min_words:
        # Classify failures carefully:
        # - login_wall if we saw paywall markers
        # - blocked only if we saw strong WAF signals AND content is too thin
        if login_wall:
            st = "login_wall"
            err = f"login_wall_thin_content (words={wc})"
        elif suspected_blocked:
            st = "blocked"
            err = f"blocked_or_waf_thin_content (words={wc}, status={http_status})"
        else:
            st = "thin_content"
            err = f"thin_content (words={wc})"
        return {
            "status": st,
            "error": err,
            "fetch_url": fetch_url,
            "canonical_url": canonical_norm,
            "http_status": http_status,
            "content_type": ct,
            "title": meta.get("title") or title_hint,
            "author": meta.get("author"),
            "site_name": meta.get("sitename"),
            "published_at": meta.get("date"),
            "language": meta.get("language"),
            "content_text": text,
            "content_html": None,
            "debug": dbg,
        }

    # Success
    return {
        "status": "ok",
        "error": None,
        "fetch_url": fetch_url,
        "canonical_url": canonical_norm,
        "http_status": http_status,
        "content_type": ct,
        "title": meta.get("title") or title_hint,
        "author": meta.get("author"),
        "site_name": meta.get("sitename"),
        "published_at": meta.get("date"),
        "language": meta.get("language"),
        "content_text": text,
        "content_html": None,
        "debug": dbg,
    }


# -----------------------------
# DB wiring
# -----------------------------

SQL_FETCH_CANDIDATES = """
SELECT
  r.id AS raw_item_id,
  r.url AS tracking_url,
  r.title AS title,
  r.content_snip AS content_snip,
  ur.final_url AS resolved_url,
  ae.status AS prev_status,
  ae.attempts AS prev_attempts,
  ae.next_retry_at AS prev_next_retry_at
FROM public.url_resolutions ur
JOIN public.raw_items r
  ON r.url = ur.tracking_url
LEFT JOIN public.article_extractions ae
  ON ae.raw_item_id = r.id
WHERE ur.status = 'ok'
  AND ur.final_url IS NOT NULL
  AND (ae.raw_item_id IS NULL OR ae.status <> 'ok')
  AND (ae.next_retry_at IS NULL OR ae.next_retry_at <= now())
  AND (ae.attempts IS NULL OR ae.attempts < %s)
ORDER BY r.fetched_at DESC
LIMIT %s;
"""

SQL_UPSERT = """
INSERT INTO public.article_extractions (
  raw_item_id, original_url, resolved_url, fetch_url, canonical_url,
  title, author, site_name, published_at, extracted_at,
  http_status, content_type, language, word_count,
  content_text, content_html, content_hash, raw_extraction_json,
  status, error, attempts, next_retry_at
) VALUES (
  %(raw_item_id)s, %(original_url)s, %(resolved_url)s, %(fetch_url)s, %(canonical_url)s,
  %(title)s, %(author)s, %(site_name)s, %(published_at)s, %(extracted_at)s,
  %(http_status)s, %(content_type)s, %(language)s, %(word_count)s,
  %(content_text)s, %(content_html)s, %(content_hash)s, %(raw_extraction_json)s,
  %(status)s, %(error)s, %(attempts)s, %(next_retry_at)s
)
ON CONFLICT (raw_item_id) DO UPDATE SET
  original_url = EXCLUDED.original_url,
  resolved_url = EXCLUDED.resolved_url,
  fetch_url = EXCLUDED.fetch_url,
  canonical_url = COALESCE(EXCLUDED.canonical_url, public.article_extractions.canonical_url),
  title = COALESCE(EXCLUDED.title, public.article_extractions.title),
  author = COALESCE(EXCLUDED.author, public.article_extractions.author),
  site_name = COALESCE(EXCLUDED.site_name, public.article_extractions.site_name),
  published_at = COALESCE(EXCLUDED.published_at, public.article_extractions.published_at),
  extracted_at = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.extracted_at
    ELSE EXCLUDED.extracted_at
  END,
  http_status = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.http_status
    ELSE EXCLUDED.http_status
  END,
  content_type = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.content_type
    ELSE EXCLUDED.content_type
  END,
  language = COALESCE(EXCLUDED.language, public.article_extractions.language),
  word_count = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.word_count
    ELSE EXCLUDED.word_count
  END,
  content_text = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.content_text
    ELSE EXCLUDED.content_text
  END,
  content_html = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.content_html
    ELSE EXCLUDED.content_html
  END,
  content_hash = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.content_hash
    ELSE EXCLUDED.content_hash
  END,
  raw_extraction_json = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.raw_extraction_json
    ELSE EXCLUDED.raw_extraction_json
  END,
  status = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.status
    ELSE EXCLUDED.status
  END,
  error = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.error
    ELSE EXCLUDED.error
  END,
  attempts = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.attempts
    ELSE EXCLUDED.attempts
  END,
  next_retry_at = CASE
    WHEN public.article_extractions.status = 'ok' AND EXCLUDED.status <> 'ok' THEN public.article_extractions.next_retry_at
    ELSE EXCLUDED.next_retry_at
  END;
"""


def load_env(dotenv_path: Optional[str]) -> None:
    if dotenv_path and load_dotenv:
        load_dotenv(dotenv_path, override=False)


def get_dsn_from_env() -> str:
    """
    Preference order:
    1) DATABASE_URL if present and looks real
    2) DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url and not any(x in db_url for x in ["USER:PASS@HOST", "<DB_USER>", "<DB_PASSWORD>", "@HOST:"]):
        return db_url

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "inip")
    user = os.getenv("DB_USER")
    pw = os.getenv("DB_PASSWORD")
    if not user or not pw:
        raise RuntimeError("Missing DB_USER/DB_PASSWORD in env (or set a valid DATABASE_URL).")
    return f"dbname={name} user={user} password={pw} host={host} port={port}"


# -----------------------------
# Run modes
# -----------------------------

async def run_db_mode_async(args: argparse.Namespace) -> int:
    if psycopg2 is None:
        raise SystemExit("Missing dependency 'psycopg2-binary'. Install with: python -m pip install psycopg2-binary")

    settings = Settings(
        timeout_s=args.timeout,
        proxy=args.proxy,
        user_agent=args.user_agent,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        use_playwright_fallback=args.use_playwright_fallback,
        playwright_profile_dir=args.playwright_profile_dir,
        playwright_headless=not args.playwright_headful,
        search_fallback=args.search_fallback,
        min_words=args.min_words,
        prefer_html_over_pdf=not args.no_prefer_html_over_pdf,
        enable_pdf=args.enable_pdf,
        cache_by_url=not args.no_cache_by_url,
        max_attempts=args.max_attempts,
        retry_base_minutes=args.retry_base_minutes,
        retry_max_hours=args.retry_max_hours,
    )

    load_env(args.dotenv_path)
    dsn = get_dsn_from_env()

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SQL_FETCH_CANDIDATES, (settings.max_attempts, args.limit))
            rows = cur.fetchall()

        if not rows:
            print("No candidates found.")
            return 0

        # Build candidate objects and optionally cache by URL
        candidates: List[Dict[str, Any]] = []
        for r in rows:
            candidates.append({
                "raw_item_id": str(r["raw_item_id"]),
                "tracking_url": r["tracking_url"],
                "resolved_url": r["resolved_url"],
                "title": r.get("title"),
                "content_snip": r.get("content_snip"),
                "prev_attempts": int(r["prev_attempts"]) if r.get("prev_attempts") is not None else 0,
            })

        # group by fetch_url for caching
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for c in candidates:
            fetch_url = normalize_url(c["resolved_url"] or c["tracking_url"])
            c["fetch_url0"] = fetch_url
            groups.setdefault(fetch_url, []).append(c)

        sem = asyncio.Semaphore(max(1, args.concurrency))
        cache: Dict[str, Dict[str, Any]] = {}
        db_lock = asyncio.Lock()

        async def _run_group(fetch_url: str, group: List[Dict[str, Any]]) -> None:
            # pick the best title hint among group
            title_hint = None
            for g in group:
                if g.get("title"):
                    title_hint = g["title"]
                    break
            # desired domain for search restrictions
            desired = None
            try:
                desired = base_domain_from_netloc(urlparse(fetch_url).netloc)
            except Exception:
                desired = None

            async with sem:
                if settings.cache_by_url and fetch_url in cache:
                    result = cache[fetch_url]
                else:
                    result = await extract_for_url(fetch_url, title_hint, settings, desired)
                    if settings.cache_by_url:
                        cache[fetch_url] = result

            # write per raw_item_id
            async with db_lock:
                with conn.cursor() as wcur:
                    for g in group:
                        attempts_total = int(g.get("prev_attempts") or 0) + 1
                        status = result["status"]
                        error = result.get("error")
                        next_retry = compute_next_retry(status, attempts_total, settings)

                        row = {
                            "raw_item_id": g["raw_item_id"],
                            "original_url": g["tracking_url"],
                            "resolved_url": g["resolved_url"],
                            "fetch_url": (result.get("fetch_url") or fetch_url),
                            "canonical_url": result.get("canonical_url"),
                            "title": result.get("title") or title_hint,
                            "author": result.get("author"),
                            "site_name": result.get("site_name"),
                            "published_at": result.get("published_at"),
                            "extracted_at": datetime.now(timezone.utc),
                            "http_status": result.get("http_status"),
                            "content_type": result.get("content_type"),
                            "language": result.get("language"),
                            "word_count": word_count(result.get("content_text") or "") if result.get("content_text") else None,
                            "content_text": result.get("content_text"),
                            "content_html": result.get("content_html"),
                            "content_hash": to_hash(result.get("content_text") or "") if result.get("content_text") else None,
                            "raw_extraction_json": json.dumps({"status": status, "error": error, "used_url": (result.get("fetch_url") or fetch_url), "debug": result.get("debug")}, ensure_ascii=False),
                            "status": status,
                            "error": error,
                            "attempts": attempts_total,
                            "next_retry_at": next_retry,
                        }
                        wcur.execute(SQL_UPSERT, row)
                conn.commit()

        tasks = []
        for fu, grp in groups.items():
            tasks.append(asyncio.create_task(_run_group(fu, grp)))
        await asyncio.gather(*tasks)
        print(f"DB run complete. Candidates: {len(candidates)} | Unique URLs: {len(groups)}")
        return 0
    finally:
        conn.close()


def run_db_mode(args: argparse.Namespace) -> int:
    return asyncio.run(run_db_mode_async(args))


def run_csv_mode(args: argparse.Namespace) -> int:
    if pd is None:
        raise SystemExit("Missing dependency 'pandas'. Install with: python -m pip install pandas")
    df = pd.read_csv(args.csv, encoding=args.csv_encoding)

    # Determine which column contains the URL we should fetch
    url_col = None
    for c in ["final_url", "fetch_url", "resolved_url", "url"]:
        if c in df.columns:
            url_col = c
            break
    if not url_col:
        raise SystemExit("CSV mode requires one of columns: final_url, fetch_url, resolved_url, url")

    title_col = "title" if "title" in df.columns else None
    # Filter out empty URLs
    df = df[df[url_col].notna()].copy()
    if args.limit:
        df = df.head(args.limit)

    settings = Settings(
        timeout_s=args.timeout,
        proxy=args.proxy,
        user_agent=args.user_agent,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        use_playwright_fallback=args.use_playwright_fallback,
        playwright_profile_dir=args.playwright_profile_dir,
        playwright_headless=not args.playwright_headful,
        search_fallback=args.search_fallback,
        min_words=args.min_words,
        prefer_html_over_pdf=not args.no_prefer_html_over_pdf,
        enable_pdf=args.enable_pdf,
        cache_by_url=not args.no_cache_by_url,
        max_attempts=args.max_attempts,
        retry_base_minutes=args.retry_base_minutes,
        retry_max_hours=args.retry_max_hours,
    )

    async def _run() -> pd.DataFrame:
        sem = asyncio.Semaphore(max(1, args.concurrency))
        cache: Dict[str, Dict[str, Any]] = {}
        db_lock = asyncio.Lock()
        out_rows: List[Dict[str, Any]] = []

        async def _one(u: str, title: Optional[str]) -> None:
            fu = normalize_url(u)
            desired = None
            try:
                desired = base_domain_from_netloc(urlparse(fu).netloc)
            except Exception:
                desired = None
            async with sem:
                if settings.cache_by_url and fu in cache:
                    res = cache[fu]
                else:
                    res = await extract_for_url(fu, title, settings, desired)
                    if settings.cache_by_url:
                        cache[fu] = res
            txt = res.get("content_text") or ""
            out_rows.append({
                "input_url": u,
                "fetch_url": res.get("fetch_url") or fu,
                "canonical_url": res.get("canonical_url"),
                "status": res.get("status"),
                "error": res.get("error"),
                "http_status": res.get("http_status"),
                "content_type": res.get("content_type"),
                "title": res.get("title") or title,
                "word_count": word_count(txt) if txt else None,
                "content_text": txt if txt else None,
                "content_hash": to_hash(txt) if txt else None,
                "raw_extraction_json": json.dumps({"debug": res.get("debug")}, ensure_ascii=False),
            })

        tasks = []
        for _, r in df.iterrows():
            u = str(r[url_col])
            title = str(r[title_col]) if title_col and pd.notna(r[title_col]) else None
            tasks.append(asyncio.create_task(_one(u, title)))
        await asyncio.gather(*tasks)

        return pd.DataFrame(out_rows)

    out_df = asyncio.run(_run())
    out_df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out}. Attempted: {len(out_df)} | OK: {(out_df['status']=='ok').sum()}")
    return 0


# -----------------------------
# CLI
# -----------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch and extract article content into article_extractions (v8).")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--db", action="store_true", help="Run in DB mode using url_resolutions + raw_items.")
    mode.add_argument("--csv", type=str, help="Run in CSV mode reading resolved URLs.")
    p.add_argument("--out", type=str, default="article_extractions_out.csv", help="CSV output in --csv mode.")
    p.add_argument("--csv-encoding", type=str, default="utf-8", help="CSV input encoding.")
    p.add_argument("--limit", type=int, default=150, help="Limit rows processed.")
    p.add_argument("--concurrency", type=int, default=1, help="Concurrency (keep low).")

    # HTTP
    p.add_argument("--timeout", type=float, default=25.0, help="HTTP timeout (seconds).")
    p.add_argument("--proxy", type=str, default=None, help="Proxy URL (optional).")
    p.add_argument("--user-agent", type=str, default=Settings.user_agent, help="User-Agent string.")

    # pacing
    p.add_argument("--delay-min", type=float, default=0.0, help="Min delay between requests (seconds).")
    p.add_argument("--delay-max", type=float, default=0.0, help="Max delay between requests (seconds).")

    # fallbacks
    p.add_argument("--use-playwright-fallback", action="store_true", help="Use Playwright when blocked/thin.")
    p.add_argument("--playwright-headful", action="store_true", help="Run Playwright headful (debug).")
    p.add_argument("--playwright-profile-dir", type=str, default=None, help="Kept for compatibility (future use).")
    p.add_argument("--search-fallback", action="store_true", help="Use DuckDuckGo HTML search when blocked/thin.")

    # extraction tuning
    p.add_argument("--min-words", type=int, default=120, help="Minimum words required to treat extraction as OK.")
    p.add_argument("--no-prefer-html-over-pdf", action="store_true", help="If set, do not try to find HTML alternatives for PDFs.")
    p.add_argument("--enable-pdf", action="store_true", help="If set, try extracting PDFs (requires pypdf).")

    # reliability / retry
    p.add_argument("--max-attempts", type=int, default=5, help="Stop retrying after this many attempts per raw_item_id.")
    p.add_argument("--retry-base-minutes", type=int, default=60, help="Base retry delay (minutes). Exponential backoff applied.")
    p.add_argument("--retry-max-hours", type=int, default=48, help="Max retry delay (hours).")
    p.add_argument("--no-cache-by-url", action="store_true", help="Disable within-run caching by fetch_url (slower).")

    # env
    p.add_argument("--dotenv-path", type=str, default=None, help="Path to .env file.")
    return p


def main(argv: Sequence[str]) -> int:
    args = build_parser().parse_args(argv)
    if args.db:
        return run_db_mode(args)
    return run_csv_mode(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))