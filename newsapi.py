"""
test_newsapi.py

NewsAPI “Apple Inc” test script:
- Loads NEWSAPI_KEY from .env (project root)
- Queries NewsAPI /v2/everything for the last 7 days
- Uses apiKey as a query param (reliable)
- Prints a clean terminal summary + top articles
- Pretty-prints JSON like the docs
- Saves:
    - raw response JSON
    - flattened articles JSON (easier to read/use)

Usage:
  .\.venv\Scripts\python.exe .\test_newsapi.py
  .\.venv\Scripts\python.exe .\test_newsapi.py --query '"Apple" OR "Apple Inc" OR AAPL'
  .\.venv\Scripts\python.exe .\test_newsapi.py --days 3 --page-size 10 --max-print 10
"""

import os
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv


def redact_api_key(url: str) -> str:
    # Redact apiKey=... from the URL so you can safely print it
    # Handles apiKey at end or in middle of querystring
    import re
    return re.sub(r"(apiKey=)[^&]+", r"\1***REDACTED***", url)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query",
        default='"Apple" OR "Apple Inc" OR AAPL -biztoc -giveawayoftheday',
        help="NewsAPI q= query string",
    )
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days")
    parser.add_argument("--page-size", type=int, default=5, help="Articles per page (max 100)")
    parser.add_argument("--page", type=int, default=1, help="Page number (pagination)")
    parser.add_argument("--max-print", type=int, default=5, help="How many articles to print nicely")
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Pretty-print full JSON response (can be verbose)",
    )
    args = parser.parse_args()

    # Load .env from current working directory (project root)
    load_dotenv()

    api_key = os.getenv("NEWSAPI_KEY")
    if not api_key:
        raise SystemExit("Missing NEWSAPI_KEY. Check your .env file in the project root.")

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": args.query,
        "from": start.date().isoformat(),
        # Do NOT set "to" unless you use a real timestamp; omitting is safest
        "sortBy": "publishedAt",
        "language": "en",
        "pageSize": args.page_size,
        "page": args.page,
        "apiKey": api_key,  # querystring auth for reliability
    }

    resp = requests.get(url, params=params, timeout=30)

    # Safe to print (redacted)
    print("Request URL:", redact_api_key(resp.url))
    print("HTTP:", resp.status_code)

    try:
        data = resp.json()
    except Exception:
        print("Non-JSON response received.")
        resp.raise_for_status()
        raise

    # Handle API errors cleanly
    if resp.status_code != 200 or data.get("status") != "ok":
        print(json.dumps(data, indent=2, ensure_ascii=False))
        raise SystemExit(1)

    total = data.get("totalResults")
    articles = data.get("articles", []) or []
    print("status:", data.get("status"))
    print("totalResults:", total)
    print("articles_len:", len(articles))

    # Nice terminal view
    print("\nTop articles:")
    for i, a in enumerate(articles[: args.max_print], 1):
        src = (a.get("source") or {}).get("name")
        print(f"{i}. {a.get('title')}")
        print(f"   {src} — {a.get('publishedAt')}")
        print(f"   {a.get('url')}\n")

    # Optional: pretty-print full JSON like the docs page
    if args.print_json:
        print(json.dumps(data, indent=2, ensure_ascii=False))

    # Save outputs (timestamped)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)

    raw_path = out_dir / f"newsapi_raw_{stamp}.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    flat_articles = []
    for a in articles:
        flat_articles.append(
            {
                "source": (a.get("source") or {}).get("name"),
                "author": a.get("author"),
                "title": a.get("title"),
                "description": a.get("description"),
                "url": a.get("url"),
                "urlToImage": a.get("urlToImage"),
                "publishedAt": a.get("publishedAt"),
                "content": a.get("content"),
            }
        )

    flat_path = out_dir / f"newsapi_articles_{stamp}.json"
    with flat_path.open("w", encoding="utf-8") as f:
        json.dump(flat_articles, f, indent=2, ensure_ascii=False)

    print(f"Saved raw JSON to:   {raw_path}")
    print(f"Saved articles to:   {flat_path}")


if __name__ == "__main__":
    main()
