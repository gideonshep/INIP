import asyncio
import logging
import os
import json
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import httpx
from trafilatura import extract, extract_metadata
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Config
current_dir = Path(__file__).parent.absolute()
env_path = current_dir.parent / '.env'
if not env_path.exists(): env_path = current_dir / '.env'
load_dotenv(dotenv_path=env_path)

try:
    db_user = os.environ.get("DB_USER", "postgres")
    db_pass = os.environ.get("DB_PASSWORD", "password")
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "inip")
    DB_DSN = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
except Exception:
    DB_DSN = os.getenv("DATABASE_URL")

class ContentFetcher:
    def __init__(self, dsn):
        self.dsn = dsn
        self.http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    async def run(self):
        pool = await asyncpg.create_pool(self.dsn)
        
        # 1. Get items to fetch
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT i.id, i.url, s.org_name 
                FROM press_release_items i
                JOIN press_release_sources s ON i.source_id = s.id
                WHERE i.content_status IN ('new', 'fetch_failed')
                AND i.url NOT LIKE '%/search%' 
                AND i.url NOT LIKE '%/archive%'
                AND i.attempt_count < 5
                LIMIT 50
            """)
        
        if not rows:
            logger.info("No items to fetch.")
            await pool.close()
            return

        logger.info(f"Fetching content for {len(rows)} items...")

        for row in rows:
            item_id = row['id']
            url = row['url']
            org_name = row['org_name']
            
            try:
                # 2. Fetch HTML
                resp = await self.http_client.get(url)
                if resp.status_code != 200:
                    await self._update_status(pool, item_id, 'fetch_failed', error=f"HTTP {resp.status_code}")
                    continue
                
                # 3. Extract Metadata & Text
                text = extract(resp.text, include_comments=False, include_tables=True)
                metadata = extract_metadata(resp.text)
                
                word_count = len(text.split()) if text else 0
                
                if not text or word_count < 50:
                    await self._update_status(pool, item_id, 'thin_content')
                    logger.warning(f"Thin content ({word_count} words): {url}")
                else:
                    # Helper to safely serialize metadata
                    meta_dict = {}
                    if metadata:
                        # manually map known fields to avoid _Element serialization errors
                        meta_dict = {
                            "title": metadata.title,
                            "author": metadata.author,
                            "url": metadata.url,
                            "hostname": metadata.hostname,
                            "description": metadata.description,
                            "sitename": metadata.sitename,
                            "date": metadata.date,
                            "categories": metadata.categories,
                            "tags": metadata.tags,
                            "fingerprint": metadata.fingerprint,
                            "id": metadata.id,
                            "license": metadata.license
                        }

                    # 4. Save Content
                    data = {
                        "item_id": item_id,
                        "fetched_url": str(resp.url),
                        "content_text": text,
                        "title": meta_dict.get('title'),
                        "author": meta_dict.get('author') or org_name,
                        "published_at": meta_dict.get('date'),
                        "site_name": meta_dict.get('sitename') or org_name,
                        "language": metadata.language if metadata else 'en',
                        "word_count": word_count,
                        "content_hash": str(hash(text)),
                        "raw_extraction_json": json.dumps(meta_dict, default=str) # Safe serialization
                    }
                    
                    await self._save_content(pool, data)
                    await self._update_status(pool, item_id, 'fetched_ok')
                    logger.info(f"Saved: {org_name} - {word_count} words")

            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                await self._update_status(pool, item_id, 'fetch_failed', error=str(e))

        await pool.close()

    async def _update_status(self, pool, item_id, status, error=None):
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE press_release_items 
                SET content_status = $1, last_error = $2, attempt_count = attempt_count + 1, next_retry_at = NOW() + interval '1 hour'
                WHERE id = $3
            """, status, error, item_id)

    async def _save_content(self, pool, data):
        query = """
            INSERT INTO press_release_content (
                item_id, fetched_url, content_text, content_hash,
                title, author, site_name, language, word_count, published_at, raw_extraction_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (item_id) DO UPDATE 
            SET content_text = EXCLUDED.content_text,
                content_hash = EXCLUDED.content_hash,
                word_count = EXCLUDED.word_count,
                raw_extraction_json = EXCLUDED.raw_extraction_json,
                extracted_at = NOW()
        """
        pub_date = None
        if data['published_at']:
            try:
                # Basic ISO parsing
                pub_date = datetime.fromisoformat(str(data['published_at']))
            except: pass

        async with pool.acquire() as conn:
            await conn.execute(query, 
                data['item_id'], 
                data['fetched_url'], 
                data['content_text'], 
                data['content_hash'],
                data['title'],
                data['author'],
                data['site_name'],
                data['language'],
                data['word_count'],
                pub_date,
                data['raw_extraction_json']
            )

if __name__ == "__main__":
    fetcher = ContentFetcher(DB_DSN)
    asyncio.run(fetcher.run())