import argparse
import json
from datetime import datetime, timezone, timedelta
import time

import feedparser
import requests

from inip.db import get_conn

PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "P4": 4}

def now_utc():
    return datetime.now(timezone.utc)

def allowed_priority(priority: str | None, min_priority: str) -> bool:
    if min_priority.upper() == "ALL":
        return True
    p = (priority or "").strip().upper()
    if p not in PRIORITY_ORDER:
        # if priority is blank/unknown: treat as lowest quality
        return False
    return PRIORITY_ORDER[p] <= PRIORITY_ORDER[min_priority.upper()]

def entry_datetime(entry):
    if getattr(entry, "published_parsed", None):
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    if getattr(entry, "updated_parsed", None):
        return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-priority", default="ALL", help="P0..P4 or ALL")
    ap.add_argument("--limit-sources", type=int, default=0, help="0 = no limit")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--sleep-ms", type=int, default=0, help="sleep between sources (politeness)")

    # NEW: control what we store
    ap.add_argument("--since-days", type=int, default=30, help="Only store items published in last N days")
    ap.add_argument("--max-items-per-feed", type=int, default=50, help="Process at most N items per feed")

    args = ap.parse_args()

    min_p = args.min_priority.upper()
    if min_p != "ALL" and min_p not in PRIORITY_ORDER:
        raise SystemExit("min-priority must be ALL or one of P0,P1,P2,P3,P4")

    cutoff = now_utc() - timedelta(days=args.since_days)

    inserted = 0
    skipped = 0
    skipped_old = 0
    failed = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, base_url, source_type, priority
                FROM sources
                WHERE active = true
                  AND source_type IN ('rss','atom')
                ORDER BY id
                """
            )
            sources = cur.fetchall()

        if args.limit_sources and args.limit_sources > 0:
            sources = sources[: args.limit_sources]

        for source_id, feed_url, source_type, priority in sources:
            if not allowed_priority(priority, min_p):
                continue

            checked_at = now_utc()
            status_code = None

            try:
                resp = requests.get(
                    feed_url,
                    timeout=args.timeout,
                    headers={"User-Agent": "INIP-MVP/0.1 (+rss-ingest)"},
                    allow_redirects=True,
                )
                status_code = resp.status_code
                resp.raise_for_status()

                feed = feedparser.parse(resp.content)
                if getattr(feed, "bozo", 0) == 1 and getattr(feed, "entries", None) is None:
                    raise RuntimeError(f"Feed parse error (bozo): {getattr(feed, 'bozo_exception', 'unknown')}")

                with conn.cursor() as cur:
                    # process limited number of entries per feed
                    for entry in feed.entries[: args.max_items_per_feed]:
                        url = getattr(entry, "link", None) or getattr(entry, "id", None)
                        if not url:
                            skipped += 1
                            continue

                        title = getattr(entry, "title", None)
                        published_at = entry_datetime(entry) or checked_at
                        summary = getattr(entry, "summary", None)

                        # NEW: date cutoff
                        if published_at < cutoff:
                            skipped_old += 1
                            continue

                        raw_json = json.dumps(entry, default=str)

                        cur.execute(
                            """
                            INSERT INTO raw_items (
                              source_id, url, title, published_at, fetched_at, raw_json, content_snip
                            )
                            VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s)
                            ON CONFLICT (url) DO NOTHING
                            """,
                            (source_id, url, title, published_at, checked_at, raw_json, summary),
                        )
                        if cur.rowcount == 1:
                            inserted += 1
                        else:
                            skipped += 1

                    # mark success (even if all entries were old)
                    cur.execute(
                        """
                        UPDATE sources
                        SET last_checked_at=%s,
                            last_ok_at=%s,
                            last_status_code=%s,
                            fail_count=0,
                            last_error=NULL
                        WHERE id=%s
                        """,
                        (checked_at, checked_at, status_code, source_id),
                    )

                conn.commit()

            except Exception as e:
                failed += 1
                error_msg = str(e)[:500]

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE sources
                        SET last_checked_at=%s,
                            last_status_code=%s,
                            fail_count=fail_count+1,
                            last_error=%s
                        WHERE id=%s
                        """,
                        (checked_at, status_code, error_msg, source_id),
                    )
                conn.commit()

                print(f"[WARN] feed failed: {feed_url} :: {error_msg}")

            if args.sleep_ms and args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

    print(f"Inserted: {inserted}")
    print(f"Skipped (duplicates/missing url): {skipped}")
    print(f"Skipped old (< {args.since_days} days): {skipped_old}")
    print(f"Failed feeds: {failed}")
    print(f"Min priority: {min_p}")
    print(f"Cutoff UTC: {cutoff.isoformat()}")

if __name__ == "__main__":
    main()
