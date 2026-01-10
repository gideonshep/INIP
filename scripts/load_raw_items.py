import argparse
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from inip.db import get_conn


def parse_published_at(s: str | None):
    if not s:
        return None
    # NewsAPI returns ISO8601 like "2026-01-03T07:06:05Z"
    # Python fromisoformat doesn't like trailing Z, so convert:
    s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def content_hash(title: str | None, url: str | None, published_at: str | None) -> str:
    base = f"{(title or '').strip()}|{(url or '').strip()}|{(published_at or '').strip()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def ensure_source(conn, name: str, source_type: str = "newsapi", base_url: str = "https://newsapi.org"):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM sources
            WHERE name = %s AND source_type = %s
            LIMIT 1
            """,
            (name, source_type),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            """
            INSERT INTO sources(name, source_type, base_url)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (name, source_type, base_url),
        )
        return cur.fetchone()[0]


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--articles",
        required=True,
        help="Path to flattened NewsAPI articles JSON (list of dicts)",
    )
    p.add_argument(
        "--source-name",
        default="NewsAPI",
        help="Name to store in sources table (default: NewsAPI)",
    )
    args = p.parse_args()

    path = Path(args.articles)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    articles = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(articles, list):
        raise SystemExit("Expected a JSON list of article objects")

    with get_conn() as conn:
        source_id = ensure_source(conn, args.source_name)

        inserted = 0
        skipped = 0

        with conn.cursor() as cur:
            for a in articles:
                url = a.get("url")
                if not url:
                    skipped += 1
                    continue

                title = a.get("title")
                pub_s = a.get("publishedAt")
                pub_dt = parse_published_at(pub_s)

                h = content_hash(title, url, pub_s)

                # Store the per-article JSON as raw_json (good for audit/replay)
                raw_json = a

                cur.execute(
                    """
                    INSERT INTO raw_items(
                        source_id, url, title, published_at, fetched_at,
                        raw_json, content_snip, content_text, content_hash
                    )
                    VALUES (%s, %s, %s, %s, NOW(), %s::jsonb, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                    """,
                    (
                        source_id,
                        url,
                        title,
                        pub_dt,
                        json.dumps(raw_json),
                        a.get("description"),
                        a.get("content"),
                        h,
                    ),
                )

                # rowcount is 1 if inserted, 0 if conflict/do nothing
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1

        conn.commit()

    print(f"Loaded file: {path.name}")
    print(f"Inserted: {inserted}")
    print(f"Skipped (duplicates/missing url): {skipped}")


if __name__ == "__main__":
    main()
