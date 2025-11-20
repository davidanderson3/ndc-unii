#!/usr/bin/env python3
"""
Build chunked JSON files for a fast, serverless HTML view.

Reads ndc_unii_rxnorm.json and writes:
  - web/data/index.json           (bucket metadata)
  - web/data/ndc_XXX.json         (records where first 3 NDC digits == XXX)

Chunking strategy:
  - Normalize NDC by stripping non-digits
  - Bucket key = first 3 digits
  - Only creates files for buckets that have records

Usage:
  python web/build_chunks.py

Then open web/index.html with a static server (e.g. `python -m http.server` from repo root).
"""
import json
import re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
SRC  = ROOT / "ndc_unii_rxnorm.json"
OUTD = ROOT / "web" / "data"

def ndc_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def bucket_key(ndc: str) -> str:
    d = ndc_digits(ndc)
    return d[:3] if d else "zzz"

def main():
    if not SRC.is_file():
        raise SystemExit(f"Missing {SRC} — run ndc_unii.py first")

    print(f"Loading {SRC.name} (this may take a moment)…")
    with open(SRC, encoding="utf-8") as f:
        data = json.load(f)

    buckets = defaultdict(list)
    search_records = []
    for rec in data:
        b = bucket_key(rec.get("ndc"))
        buckets[b].append(rec)
        # Collect UNIIs across ingredients to enable lightweight search index
        uniis = set()
        for ing in rec.get("ingredients", []) or []:
            if ing.get("unii"):
                uniis.add(ing["unii"])
        search_records.append(
            {
                "bucket": b,
                "ndc": rec.get("ndc"),
                "rxcui": rec.get("rxcui"),
                "name": rec.get("str"),
                "unii": sorted(uniis),
            }
        )

    OUTD.mkdir(parents=True, exist_ok=True)

    meta = {"bucket_size": "first3digits", "buckets": {}}
    for b, items in buckets.items():
        if b == "zzz":
            # Unknown/short NDCs: write as ndc_zzz.json to keep logic simple
            pass
        p = OUTD / f"ndc_{b}.json"
        with open(p, "w", encoding="utf-8") as f:
            json.dump(items, f, separators=(",", ":"))  # compact
        meta["buckets"][b] = len(items)
        print(f"Wrote {p.relative_to(ROOT)}: {len(items)} records")

    with open(OUTD / "index.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print(f"Wrote data/index.json with {len(meta['buckets'])} buckets")

    # Lightweight search index for name/RxCUI/UNII filtering (keeps buckets small)
    with open(OUTD / "search_index.json", "w", encoding="utf-8") as f:
        json.dump({"records": search_records}, f, separators=(",", ":"))
    print(f"Wrote data/search_index.json with {len(search_records)} records")

if __name__ == "__main__":
    main()
