import argparse
import json
import re
from datetime import datetime, timezone, timedelta

from inip.db import get_conn

def now_utc():
    return datetime.now(timezone.utc)

# --- simple rule dictionaries (MVP) ---
LOB_RULES = [
    ("Motor", re.compile(r"\b(car|vehicle|motor|van|truck|hgv|theft|stolen|keyless|range rover|land rover)\b", re.I)),
    ("Property", re.compile(r"\b(flood|storm|subsidence|fire|arson|burst pipe|escape of water)\b", re.I)),
    ("SME", re.compile(r"\b(business interruption|warehouse|commercial property|retail premises)\b", re.I)),
    ("Cyber", re.compile(r"\b(ransomware|data breach|phishing|cyber attack)\b", re.I)),
]

PERIL_RULES = [
    ("Theft", re.compile(r"\b(theft|stolen|stole|carjacking|keyless|relay attack|gameboy)\b", re.I)),
    ("Flood", re.compile(r"\b(flood|flooding|river levels|flood warning)\b", re.I)),
    ("Fire", re.compile(r"\b(fire|arson|lithium|battery fire|explosion)\b", re.I)),
    ("Weather", re.compile(r"\b(red warning|amber warning|storm|gale|severe weather)\b", re.I)),
    ("Regulatory", re.compile(r"\b(fca|prai?|consultation|policy statement|guidance|final rules|regulation)\b", re.I)),
    ("Fraud", re.compile(r"\b(fraud|scam|bogus|identity theft|crash for cash)\b", re.I)),
    ("Cyber", re.compile(r"\b(ransomware|data breach|ddos|malware)\b", re.I)),
]

SIGNAL_TYPE_RULES = [
    ("REGULATORY", re.compile(r"\b(fca|pra|hm treasury|consultation|policy statement|guidance|regulation|legislation)\b", re.I)),
    ("MACRO", re.compile(r"\b(inflation|interest rate|gdp|unemployment|cpi|rpi)\b", re.I)),
    ("MARKET", re.compile(r"\b(pricing|premium|rate rise|capacity|reinsurance|hard market|soft market)\b", re.I)),
    ("TECH", re.compile(r"\b(cyber|ransomware|software|ai|llm)\b", re.I)),
    ("RISK", re.compile(r"\b(theft|flood|storm|fire|arson|claims spike|losses)\b", re.I)),
]

# rating/pricing factors you mentioned (MVP extraction)
VEH_MAKE = re.compile(r"\b(toyota|bmw|audi|mercedes|ford|vauxhall|tesla|land rover|jaguar|nissan|honda|vw|volkswagen)\b", re.I)
VEH_MODEL = re.compile(r"\b(range rover|defender|fiesta|golf|polo|3 series|a3|a4|c class|model 3|model y)\b", re.I)
DRIVER_AGE = re.compile(r"\b(age(d)?\s*(\d{1,2})|\b(\d{1,2})\s*year[-\s]*old\b)\b", re.I)
POSTCODE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b", re.I)

def classify(text: str):
    text_l = text or ""
    lob = None
    peril = None
    signal_type = None

    for lob_name, pat in LOB_RULES:
        if pat.search(text_l):
            lob = lob_name
            break

    for peril_name, pat in PERIL_RULES:
        if pat.search(text_l):
            peril = peril_name
            break

    for stype, pat in SIGNAL_TYPE_RULES:
        if pat.search(text_l):
            signal_type = stype
            break

    # default: if we found a peril but no type, call it RISK
    if signal_type is None and peril is not None:
        signal_type = "RISK"

    # severity heuristic (very crude MVP)
    severity = None
    if re.search(r"\b(red warning|record|surge|spike|epidemic|wave)\b", text_l, re.I):
        severity = 4
    if re.search(r"\b(death|killed|fatal|catastrophic|major incident)\b", text_l, re.I):
        severity = 5
    if severity is None and (peril or signal_type):
        severity = 3

    is_relevant = bool(peril or signal_type or lob)

    rating_factors = {}
    makes = sorted(set(m.group(0).lower() for m in VEH_MAKE.finditer(text_l)))
    models = sorted(set(m.group(0).lower() for m in VEH_MODEL.finditer(text_l)))
    postcodes = sorted(set(m.group(0).upper().replace(" ", "") for m in POSTCODE.finditer(text_l)))

    if makes:
        rating_factors["vehicle_make"] = makes
    if models:
        rating_factors["vehicle_model"] = models
    if postcodes:
        rating_factors["postcode"] = postcodes

    # driver age: take any numeric captures
    ages = []
    for m in DRIVER_AGE.finditer(text_l):
        nums = [x for x in m.groups() if x and x.isdigit()]
        for n in nums:
            ages.append(int(n))
    ages = sorted(set([a for a in ages if 16 <= a <= 100]))
    if ages:
        rating_factors["driver_age"] = ages

    return {
        "is_relevant": is_relevant,
        "signal_type": signal_type,
        "line_of_business": lob,
        "peril": peril,
        "severity": severity,
        "rating_factors": rating_factors if rating_factors else None,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-days", type=int, default=30, help="Process raw_items from last N days")
    ap.add_argument("--limit", type=int, default=1000, help="Max raw_items to process per run")
    args = ap.parse_args()

    cutoff = now_utc() - timedelta(days=args.since_days)

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Only raw_items without an existing signal (idempotent behavior)
            cur.execute(
                """
                SELECT r.id, r.title, r.content_snip, r.url, r.published_at
                FROM raw_items r
                LEFT JOIN signals s ON s.raw_item_id = r.id
                WHERE s.raw_item_id IS NULL
                  AND r.published_at >= %s
                ORDER BY r.published_at DESC NULLS LAST
                LIMIT %s
                """,
                (cutoff, args.limit),
            )
            rows = cur.fetchall()

        inserted = 0
        for raw_item_id, title, content_snip, url, published_at in rows:
            text = f"{title or ''}\n{content_snip or ''}\n{url or ''}"
            cls = classify(text)

            # keep summary lightweight for rules stage
            summary = title or content_snip

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO signals (
                      raw_item_id, is_relevant, severity, line_of_business, peril, geo,
                      affected_entity, key_risk_indicator, summary, supporting_quote,
                      confidence, model_name, prompt_version, signal_type, rating_factors
                    )
                    VALUES (
                      %s,%s,%s,%s,%s,%s,
                      %s,%s,%s,%s,
                      %s,%s,%s,%s,%s::jsonb
                    )
                    ON CONFLICT (raw_item_id) DO NOTHING
                    """,
                    (
                        raw_item_id,
                        cls["is_relevant"],
                        cls["severity"],
                        cls["line_of_business"],
                        cls["peril"],
                        None,  # geo: later
                        None,  # affected_entity: later
                        None,  # KRI: later
                        summary,
                        None,  # supporting_quote: later
                        "LOW",  # rules stage confidence
                        "rules_v1",
                        "v1",
                        cls["signal_type"],
                        json.dumps(cls["rating_factors"]) if cls["rating_factors"] else None,
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1

        conn.commit()

    print(f"Processed raw_items: {len(rows)}")
    print(f"Inserted signals: {inserted}")
    print(f"Cutoff UTC: {cutoff.isoformat()}")

if __name__ == "__main__":
    main()
