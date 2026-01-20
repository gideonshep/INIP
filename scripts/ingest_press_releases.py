import asyncio
import logging
import csv
import os
import json
import argparse
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from pathlib import Path

# Third-party libs
import asyncpg
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
current_dir = Path(__file__).parent.absolute()
env_path = current_dir.parent / '.env'
if not env_path.exists():
    env_path = current_dir / '.env'

load_dotenv(dotenv_path=env_path)

try:
    db_user = os.environ.get("DB_USER", "postgres")
    db_pass = os.environ.get("DB_PASSWORD", "password")
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "inip")
except KeyError as e:
    raise RuntimeError(f"Missing required environment variable: {e}")

DB_DSN = os.getenv("DATABASE_URL")
if not DB_DSN:
    DB_DSN = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

# ---------------------------------------------------------
# DATABASE CLASS
# ---------------------------------------------------------

class Database:
    def __init__(self, dsn):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def get_due_sources(self):
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT * FROM press_release_sources 
                WHERE is_enabled = true
            """)

    async def upsert_source(self, source):
        hint = {}
        
        # 1. Use manual selector if provided
        if source.get('selector') and len(source['selector']) > 1:
            hint['item_selector'] = source['selector']
        
        # 2. Fallback
        elif "aviva.com" in source['url']:
            hint = {"wait_for": ".search-results", "item_selector": "h3 a"}
        elif "mynewsdesk.com" in source['url']:
            hint = {"item_selector": "a.news-item-link, .news-list-item a"}
        
        site_type = 'dynamic' if 'Dynamic' in source['tech'] else 'static'

        query = """
            INSERT INTO press_release_sources 
            (org_name, category, market_rank, list_url, site_type, parser_hint)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (list_url) DO UPDATE
            SET org_name = EXCLUDED.org_name,
                site_type = EXCLUDED.site_type,
                parser_hint = EXCLUDED.parser_hint;
        """

        async with self.pool.acquire() as conn:
            await conn.execute(query, 
                source['name'], source['category'], source['rank'], 
                source['url'], site_type, json.dumps(hint)
            )

    async def save_discovered_item(self, source_id, url, title, published_at):
        query = """
            INSERT INTO press_release_items (source_id, url, title, published_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (url) DO NOTHING
            RETURNING id;
        """
        async with self.pool.acquire() as conn:
            val = await conn.fetchval(query, source_id, url, title, published_at)
            if val:
                logger.info(f"NEW ITEM: {url}")
                return True
            return False

# ---------------------------------------------------------
# DISCOVERY ENGINE
# ---------------------------------------------------------

class DiscoveryEngine:
    def __init__(self, db: Database):
        self.db = db
        self.http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    async def run(self):
        sources = await self.db.get_due_sources()
        logger.info(f"Starting discovery for {len(sources)} sources")
        
        static_sources = [s for s in sources if s['site_type'] == 'static']
        dynamic_sources = [s for s in sources if s['site_type'] == 'dynamic']

        results = {"success": 0, "failed": 0, "zero_items": []}

        for source in static_sources:
            try:
                count = await self.process_static_source(source)
                if count > 0: results["success"] += 1
                else: results["zero_items"].append(source['org_name'])
            except Exception as e:
                results["failed"] += 1
                logger.error(f"Failed static source {source['org_name']}: {e}")

        if dynamic_sources:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                
                for source in dynamic_sources:
                    try:
                        page = await context.new_page()
                        count = await self.process_dynamic_source(source, page)
                        if count > 0: results["success"] += 1
                        else: results["zero_items"].append(source['org_name'])
                        await page.close()
                    except Exception as e:
                        results["failed"] += 1
                        logger.error(f"Failed dynamic source {source['org_name']}: {e}")
                
                await browser.close()

        logger.info(f"Complete. Success: {results['success']}, Zero Items: {len(results['zero_items'])}")

    async def process_static_source(self, source):
        logger.info(f"Fetching Static: {source['org_name']}")
        try:
            resp = await self.http_client.get(source['list_url'])
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {source['org_name']}")
                return 0
            soup = BeautifulSoup(resp.text, 'html.parser')
            items = self._parse_items(soup, source)
            
            for item in items:
                await self.db.save_discovered_item(source['id'], item['url'], item['title'], None)
            return len(items)
        except Exception as e:
            logger.error(f"Error fetching {source['org_name']}: {e}")
            return 0

    async def process_dynamic_source(self, source, page: Page):
        logger.info(f"Fetching Dynamic: {source['org_name']}")
        try:
            # Increase timeout for heavy sites
            await page.goto(source['list_url'], wait_until="domcontentloaded", timeout=60000)
            
            hints = json.loads(source['parser_hint'])
            if hints.get('wait_for'):
                try:
                    await page.wait_for_selector(hints['wait_for'], timeout=10000)
                except:
                    pass
            
            if "aviva.com" in source['list_url']:
                 await page.wait_for_load_state("networkidle")

            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            items = self._parse_items(soup, source)
            
            logger.info(f"Found {len(items)} items")
            for item in items:
                await self.db.save_discovered_item(source['id'], item['url'], item['title'], None)
            return len(items)
        except Exception as e:
            logger.error(f"Dynamic fetch error {source['org_name']}: {e}")
            return 0

    def _parse_items(self, soup, source):
        hints = json.loads(source['parser_hint'])
        candidates = []
        base_url = source['list_url']

        # 1. Targeted Extraction (PRIORITY)
        if hints.get('item_selector'):
            selector = hints['item_selector']
            
            # --- SELF-HEALING LOGIC ---
            exact_matches = soup.select(selector)
            
            generalized_matches = []
            if len(exact_matches) == 1:
                element = exact_matches[0]
                parent = element.parent
                if parent:
                    tag_name = element.name
                    siblings = parent.find_all(tag_name, recursive=False)
                    if len(siblings) < 3 and parent.parent:
                         generalized_matches = parent.parent.find_all('a', href=True)
                    else:
                         for sib in siblings:
                             if sib.name == 'a': generalized_matches.append(sib)
                             else: generalized_matches.extend(sib.find_all('a'))

            links = generalized_matches if generalized_matches else exact_matches
            
            if links:
                for link in links:
                    href = link.get('href')
                    text = link.get_text(strip=True)
                    if href and text:
                        full_url = urljoin(base_url, href)
                        if not any(c['url'] == full_url for c in candidates):
                            candidates.append({"url": full_url, "title": text})
                
                if candidates:
                    logger.info(f"Targeted extraction found {len(candidates)} items (Healed)")
                    return candidates 

        # 2. Aggressive Heuristic Extraction (Fallback)
        # This now accepts ANY link that looks like content, not just "Press Releases"
        logger.info(f"Falling back to aggressive heuristics for {source['org_name']}")
        
        # We look for a "Main Content" area first to avoid headers/footers
        main_content = soup.find('main') or soup.find('article') or soup.find('div', id=re.compile('content|main')) or soup
        
        for a in main_content.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True)
            
            # Bare minimum filters (Noise reduction)
            if not text or len(text) < 15: continue # Titles are usually sentences
            if any(x in text.lower() for x in ['read more', 'click here', 'skip to', 'cookie', 'privacy', 'login', 'sign up', 'contact us', 'sitemap']): continue
            if any(x in href.lower() for x in ['javascript:', 'mailto:', 'tel:', '.pdf', '.jpg', '/contact', '/about-us']): continue
            
            full_url = urljoin(base_url, href)
            
            # --- THE CHANGE: Accept Broad Patterns ---
            is_likely_article = False
            
            # Path Check: Is it a sub-page of where we are?
            # e.g. List: /media-centre/  Item: /media-centre/article-1
            if urlparse(base_url).path in urlparse(full_url).path:
                is_likely_article = True
            
            # Keywords (Expanded)
            url_lower = full_url.lower()
            if any(x in url_lower for x in ['/news/', '/insights/', '/blog/', '/updates/', '/articles/', '/story/', '/press/']):
                is_likely_article = True
            
            # Title Length (Articles usually have long, descriptive titles)
            if len(text.split()) > 4: 
                is_likely_article = True

            if is_likely_article and not any(c['url'] == full_url for c in candidates):
                candidates.append({"url": full_url, "title": text})
                
        # Return MORE candidates (up to 50) since we are being aggressive
        return candidates[:50]

def read_csv_robust(file_path):
    encodings = ['utf-8', 'utf-8-sig', 'windows-1252', 'latin-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError: continue
        except Exception: return []
    return []

async def sync_sources_from_csv(db: Database, csv_path: str):
    logger.info(f"Syncing sources from {csv_path}")
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return

    rows = read_csv_robust(csv_path)
    count = 0
    for row in rows:
        url = row.get('Validated Target URL') or row.get('URL')
        name = row.get('Entity Name') or row.get('Name')
        tech = row.get('Ingestion Tech') or row.get('Tech')
        selector = row.get('Article Selector') or row.get('Selector') or row.get('CSS Selector')

        if url and name:
            source = {
                "name": name,
                "category": row.get('Category', ''),
                "rank": row.get('Market Rank / Focus', ''),
                "url": url.strip(),
                "tech": tech or 'Static',
                "selector": selector.strip() if selector else None
            }
            await db.upsert_source(source)
            count += 1
    logger.info(f"Synced {count} sources.")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sync', action='store_true', help='Sync CSV to DB')
    parser.add_argument('--discover', action='store_true', help='Run discovery scan')
    parser.add_argument('--csv', type=str, default='master_press_list.csv', help='Path to source CSV')
    
    args = parser.parse_args()

    db = Database(DB_DSN)
    await db.connect()

    if args.sync:
        await sync_sources_from_csv(db, args.csv)
    
    if args.discover:
        engine = DiscoveryEngine(db)
        await engine.run()

    await db.close()

if __name__ == "__main__":
    asyncio.run(main())