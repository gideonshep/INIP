import argparse
import hashlib
import json
import logging
import sys
import os
import time  # <--- Essential for sleep()
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Generator, List, Optional
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
import urllib3
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

# --- Fix: Add project root to sys.path ---
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from inip.db import get_conn

# --- Configuration & Constants ---

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("newsnow_bridge")

# Suppress SSL warnings if we use verify=False (helps with VPNs/Proxies)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Proven User-Agent for fetching the feed
USER_AGENT = "Mozilla/5.0 (compatible; INIP-Bot/0.1; +http://example.com/bot)"

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "msclkid"
}

# --- DB Helper (Source Registry) ---

def ensure_newsnow_source(conn, topic_name: str, topic_url: str) -> str:
    source_name = f"NewsNow: {topic_name}"
    source_type = "newsnow_topic"
    
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM sources WHERE source_type = %s AND base_url = %s",
            (source_type, topic_url)
        )
        row = cur.fetchone()
        if row:
            return row[0]
            
        logger.info(f"Registering new source: {source_name}")
        meta = json.dumps({"description": "NewsNow Aggregated Topic", "original_topic": topic_name})
        cur.execute(
            """
            INSERT INTO sources (name, source_type, base_url, active, meta, domain)
            VALUES (%s, %s, %s, true, %s, 'newsnow.co.uk')
            RETURNING id
            """,
            (source_name, source_type, topic_url, meta)
        )
        return cur.fetchone()[0]

# --- Core Logic: The Bridge ---

class NewsNowBridge:
    def __init__(self, timeout: int = 15):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.timeout = timeout

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_page(self, url: str) -> str:
        # verify=False helps if running through aggressive VPNs/Proxies
        resp = self.session.get(url, timeout=self.timeout, verify=False)
        resp.raise_for_status()
        return resp.text

    def resolve_redirect(self, url: str) -> str:
        """
        Pass-through: We act as an aggregator. 
        We store the NewsNow tracking URL (c.newsnow.co.uk) for now.
        
        Reason: NewsNow uses aggressive bot detection (interstitials) that require 
        heavyweight browser automation to bypass. We defer this to a downstream process.
        """
        return url

    def canonicalize_url(self, url: str) -> str:
        """
        Strip standard tracking params to keep the DB clean, 
        even if it's just the c.newsnow URL.
        """
        try:
            parsed = urlparse(url)
            q = parse_qsl(parsed.query)
            q = [(k, v) for k, v in q if k.lower() not in TRACKING_PARAMS]
            sorted_q = sorted(q)
            clean_query = urlencode(sorted_q)
            
            clean_url = urlunparse((
                parsed.scheme,
                parsed.netloc.lower(),
                parsed.path,
                parsed.params,
                clean_query,
                ""
            ))
            return clean_url
        except Exception:
            return url

    def compute_hash(self, url: str, title: str, snip: str) -> str:
        base = f"{url}||{title or ''}||{snip or ''}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    def parse(self, html: str) -> Generator[Dict, None, None]:
        soup = BeautifulSoup(html, "html.parser")
        headlines = soup.select(".newsfeed .hl")
        
        if not headlines:
            headlines = soup.select("a.hll")

        for hl in headlines:
            try:
                link_tag = hl if hl.name == 'a' else hl.select_one("a.hll")
                if not link_tag: continue
                    
                raw_url = link_tag.get("href")
                title = link_tag.get_text(strip=True)
                
                if not raw_url or not title: continue
                
                publisher = ""
                pub_tag = hl.select_one(".src")
                if pub_tag:
                    publisher = pub_tag.get_text(strip=True)
                
                time_text = ""
                time_tag = hl.select_one(".time")
                if time_tag:
                    time_text = time_tag.get_text(strip=True)

                yield {
                    "raw_url": raw_url,
                    "title": title,
                    "publisher": publisher,
                    "time_text": time_text
                }

            except Exception as e:
                logger.debug(f"Error parsing item: {e}")
                continue

# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description="Ingest NewsNow Topics")
    parser.add_argument("--topics", default="config/newsnow_topics.json", help="Path to JSON config of topics")
    parser.add_argument("--dry-run", action="store_true", help="Parse but do not insert")
    parser.add_argument("--single-url", help="Run a single topic URL immediately")
    parser.add_argument("--single-name", help="Name for the single topic", default="AdHoc")
    args = parser.parse_args()

    topics = []
    if args.single_url:
        topics.append({"name": args.single_name, "url": args.single_url})
    else:
        config_path = Path(args.topics)
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                topics = json.load(f)
        else:
            logger.warning(f"Config file not found: {args.topics}. Using default fallback.")
            topics = [
                {"name": "Insurance Industry", "url": "https://www.newsnow.co.uk/h/Industry+Sectors/Insurance"},
            ]

    bridge = NewsNowBridge()
    conn = get_conn()

    try:
        for topic in topics:
            topic_name = topic["name"]
            topic_url = topic["url"]
            
            logger.info(f"Processing Topic: {topic_name} ({topic_url})")

            if args.dry_run:
                source_id = "DRY_RUN_ID"
            else:
                source_id = ensure_newsnow_source(conn, topic_name, topic_url)

            try:
                html = bridge.fetch_page(topic_url)
                items = list(bridge.parse(html))
                
                if not items:
                    logger.warning("Found 0 items. Check if structure changed or IP blocked.")
                else:
                    logger.info(f"Found {len(items)} items raw.")
                    
            except Exception as e:
                logger.error(f"Failed to fetch {topic_name}: {e}")
                continue

            inserted_count = 0
            
            for item in items:
                # SKIP resolution for now. Using raw tracking URL.
                final_url = bridge.canonicalize_url(item["raw_url"])
                
                # Approximate fetch time as published time
                now = datetime.now(timezone.utc)
                content_snip = f"{item['publisher']} - {item['time_text']}"
                content_hash = bridge.compute_hash(final_url, item["title"], content_snip)
                
                raw_json = {
                    "original_url": item["raw_url"],
                    "time_text": item["time_text"],
                    "publisher": item["publisher"],
                    "topic_url": topic_url,
                    "note": "Unresolved tracking URL"
                }

                if args.dry_run:
                    logger.info(f"[Dry Run] Would insert: {final_url} | {item['title']}")
                    continue

                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO raw_items 
                            (source_id, url, title, published_at, fetched_at, raw_json, content_snip, content_hash)
                            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                            ON CONFLICT (url) DO NOTHING
                            """,
                            (
                                source_id, 
                                final_url, 
                                item["title"], 
                                now, 
                                now, 
                                json.dumps(raw_json),
                                content_snip,
                                content_hash
                            )
                        )
                        if cur.rowcount > 0:
                            inserted_count += 1
                    conn.commit()
                except Exception as db_err:
                    conn.rollback()
                    logger.error(f"DB Error on item {final_url}: {db_err}")

            logger.info(f"Topic {topic_name} complete. Inserted {inserted_count} new items.")
            time.sleep(2)

    finally:
        conn.close()

if __name__ == "__main__":
    main()