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
REPORT = OUTD / "patterns_report.json"
SINGLE_REPORT = OUTD / "single_report.json"
MULTI_REPORT = OUTD / "multi_report.json"

def ndc_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def bucket_key(ndc: str) -> str:
    d = ndc_digits(ndc)
    return d[:3] if d else "zzz"

def tokens(s: str):
    return [t for t in re.split(r"[^a-z0-9]+", (s or "").lower()) if t]

def cleaned(s: str):
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def main():
    if not SRC.is_file():
        raise SystemExit(f"Missing {SRC} — run ndc_unii.py first")

    print(f"Loading {SRC.name} (this may take a moment)…")
    with open(SRC, encoding="utf-8") as f:
        data = json.load(f)

    buckets = defaultdict(list)
    search_records = []
    pattern_counts = defaultdict(int)
    single_counts = defaultdict(int)
    multi_pair_stats = defaultdict(lambda: {"count": 0, "examples": []})
    records_with_pairs = 0
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
        ings = rec.get("ingredients") or []
        if len(ings) == 2:
            ttys = {ing.get("tty") for ing in ings}
            if ttys == {"IN", "PIN"}:
                key = tuple(
                    (ing.get("tty"), bool(ing.get("active_ingredient")), bool(ing.get("active_moiety")), bool(ing.get("basis_of_strength")))
                    for ing in ings
                )
                pattern_counts[key] += 1
        elif len(ings) == 1:
            ing = ings[0]
            key = (
                ing.get("tty"),
                bool(ing.get("active_ingredient")),
                bool(ing.get("active_moiety")),
                bool(ing.get("basis_of_strength")),
            )
            single_counts[key] += 1

        # Pair IN/PIN within multi-ingredient records (>=2) using simple similarity
        ins  = [ing for ing in ings if ing.get("tty") == "IN"]
        pins = [ing for ing in ings if ing.get("tty") == "PIN"]
        if len(ings) >= 2 and ins and pins:
            made_pair = False
            for pin in pins:
                best_in = None
                best_score = 0.0
                pin_clean = cleaned(pin.get("str", ""))
                pin_tokens = set(tokens(pin.get("str", "")))
                for inn in ins:
                    in_clean = cleaned(inn.get("str", ""))
                    in_tokens = set(tokens(inn.get("str", "")))
                    score = 0.0
                    if in_clean and pin_clean and in_clean in pin_clean:
                        score = 2.0  # strong substring signal
                    if in_tokens and pin_tokens:
                        overlap = len(in_tokens & pin_tokens)
                        if in_tokens:
                            score = max(score, overlap / len(in_tokens))
                    if score > best_score:
                        best_score = score
                        best_in = inn
                if best_in and best_score > 0:
                    made_pair = True
                    key = (
                        (best_in.get("tty"), bool(best_in.get("active_ingredient")), bool(best_in.get("active_moiety")), bool(best_in.get("basis_of_strength"))),
                        (pin.get("tty"), bool(pin.get("active_ingredient")), bool(pin.get("active_moiety")), bool(pin.get("basis_of_strength"))),
                    )
                    stat = multi_pair_stats[key]
                    stat["count"] += 1
                    if len(stat["examples"]) < 3:
                        stat["examples"].append({
                            "ndc": rec.get("ndc"),
                            "name": rec.get("str"),
                            "in": {
                                "rxcui": best_in.get("rxcui"),
                                "str": best_in.get("str"),
                                "unii": best_in.get("unii"),
                                "active_ingredient": bool(best_in.get("active_ingredient")),
                                "active_moiety": bool(best_in.get("active_moiety")),
                                "basis_of_strength": bool(best_in.get("basis_of_strength")),
                            },
                            "pin": {
                                "rxcui": pin.get("rxcui"),
                                "str": pin.get("str"),
                                "unii": pin.get("unii"),
                                "active_ingredient": bool(pin.get("active_ingredient")),
                                "active_moiety": bool(pin.get("active_moiety")),
                                "basis_of_strength": bool(pin.get("basis_of_strength")),
                            },
                        })
            if made_pair:
                records_with_pairs += 1

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

    if pattern_counts:
        patterns = []
        for key, count in sorted(pattern_counts.items(), key=lambda kv: kv[1], reverse=True):
            patterns.append(
                {
                    "count": count,
                    "ingredients": [
                        {
                            "tty": tty,
                            "active_ingredient": ai,
                            "active_moiety": am,
                            "basis_of_strength": bos,
                        }
                        for (tty, ai, am, bos) in key
                    ],
                }
            )
        report = {
            "total_two_ingredient_records": sum(pattern_counts.values()),
            "unique_patterns": len(pattern_counts),
            "patterns": patterns,
        }
        with open(REPORT, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"Wrote data/patterns_report.json with {len(pattern_counts)} patterns")

    if single_counts:
        singles = []
        for key, count in sorted(single_counts.items(), key=lambda kv: kv[1], reverse=True):
            tty, ai, am, bos = key
            singles.append(
                {
                    "count": count,
                    "ingredient": {
                        "tty": tty,
                        "active_ingredient": ai,
                        "active_moiety": am,
                        "basis_of_strength": bos,
                    },
                }
            )
        report = {
            "total_single_ingredient_records": sum(single_counts.values()),
            "unique_patterns": len(single_counts),
            "patterns": singles,
        }
        with open(SINGLE_REPORT, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"Wrote data/single_report.json with {len(single_counts)} patterns")

    if multi_pair_stats:
        patterns = []
        for key, stat in sorted(multi_pair_stats.items(), key=lambda kv: kv[1]["count"], reverse=True):
            patterns.append(
                {
                    "count": stat["count"],
                    "pair": [
                        {
                            "tty": tty,
                            "active_ingredient": ai,
                            "active_moiety": am,
                            "basis_of_strength": bos,
                        }
                        for (tty, ai, am, bos) in key
                    ],
                    "examples": stat["examples"],
                }
            )
        report = {
            "total_pairs": sum(stat["count"] for stat in multi_pair_stats.values()),
            "records_with_pairs": records_with_pairs,
            "unique_patterns": len(multi_pair_stats),
            "patterns": patterns,
        }
        with open(MULTI_REPORT, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"Wrote data/multi_report.json with {len(multi_pair_stats)} patterns")

if __name__ == "__main__":
    main()
