import json
import subprocess
import sys
import html
from collections import Counter

import pytest

import ndc_unii


def build_expected():
    tty_map, name_map, unii_map = ndc_unii.load_conso(ndc_unii.RXNCONSO)
    ndc_direct = ndc_unii.load_ndc_direct(ndc_unii.RXNSAT)
    sbd_to_scd, pack_to_scd, scd_to_scdc, scdc_to_in, scdc_to_pin = ndc_unii.load_rel_maps(ndc_unii.RXNREL, tty_map)
    am_map, ai_map, boss_map = ndc_unii.load_scd_attrs(ndc_unii.RXNSAT)

    out = []
    for ndc, direct_rxcuis in ndc_direct.items():
        for direct in sorted(direct_rxcuis):
            dtty = tty_map.get(direct)
            if dtty not in ndc_unii.DIRECT_TTYS:
                continue
            scd = None
            if dtty == "SCD":
                scd = direct
            elif dtty == "SBD":
                scd = sbd_to_scd.get(direct)
            elif dtty in {"GPCK", "BPCK"}:
                scd = pack_to_scd.get(direct)
            if not scd:
                continue
            ingredients = []
            seen = set()
            am_by_scdc = am_map.get(scd, {})
            ai_by_scdc = ai_map.get(scd, {})
            boss_by_scdc = boss_map.get(scd, {})
            for scdc in scd_to_scdc.get(scd, ()):   # SCDC level
                ai_target = ai_by_scdc.get(scdc)
                am_target = am_by_scdc.get(scdc)
                boss_key = boss_by_scdc.get(scdc)
                if boss_key == "AI":
                    boss_target = ai_target
                elif boss_key == "AM":
                    boss_target = am_target
                else:
                    boss_target = boss_key
                for pin in scdc_to_pin.get(scdc, ()):   # PIN ingredients
                    key = (scdc, pin)
                    if key in seen:
                        continue
                    seen.add(key)
                    ingredients.append({
                        "tty": "PIN",
                        "rxcui": pin,
                        "str": name_map.get(pin, ""),
                        "unii": unii_map.get(pin),
                        "active_ingredient": pin == ai_target,
                        "active_moiety": pin == am_target,
                        "basis_of_strength": pin == boss_target,
                        "scdc": scdc,
                    })
                for inn in scdc_to_in.get(scdc, ()):   # IN ingredients
                    key = (scdc, inn)
                    if key in seen:
                        continue
                    seen.add(key)
                    ingredients.append({
                        "tty": "IN",
                        "rxcui": inn,
                        "str": name_map.get(inn, ""),
                        "unii": unii_map.get(inn),
                        "active_ingredient": inn == ai_target,
                        "active_moiety": inn == am_target,
                        "basis_of_strength": inn == boss_target,
                        "scdc": scdc,
                    })
            if not ingredients:
                continue
            # Sort ingredients deterministically to mirror script output
            ingredients.sort(key=lambda ing: (ing["scdc"], ing["tty"], ing["rxcui"]))
            out.append({
                "ndc": ndc,
                "tty": dtty,
                "rxcui": direct,
                "str": name_map.get(direct, ""),
                "ingredients": ingredients,
            })
    # Sort final records as script does
    out.sort(key=lambda rec: (rec["ndc"], rec["tty"], rec["rxcui"]))
    return out


def summarize_counts(data):
    """Return counts for key data elements in *data* list."""
    counts = {
        "records": len(data),
        "unique_ndc": len({rec["ndc"] for rec in data}),
        "tty": dict(Counter(rec["tty"] for rec in data)),
    }
    total_ingredients = 0
    ingredient_tty = Counter()
    active_ingredient = active_moiety = basis_of_strength = 0
    for rec in data:
        ings = rec.get("ingredients", [])
        total_ingredients += len(ings)
        for ing in ings:
            ingredient_tty[ing["tty"]] += 1
            if ing.get("active_ingredient"):
                active_ingredient += 1
            if ing.get("active_moiety"):
                active_moiety += 1
            if ing.get("basis_of_strength"):
                basis_of_strength += 1
    counts.update(
        {
            "total_ingredients": total_ingredients,
            "ingredient_tty": dict(ingredient_tty),
            "active_ingredient": active_ingredient,
            "active_moiety": active_moiety,
            "basis_of_strength": basis_of_strength,
        }
    )
    return counts


def test_json_matches_rrf():
    """Run ndc_unii.py and compare its JSON output to the RxNorm RRF files.

    The test emits an HTML report describing exactly what was executed and
    compared, including record counts for each stage. The report is written to
    ``test_json_matches_rrf_report.html`` in the repository root.
    """

    steps = []
    try:
        steps.append("Executing ndc_unii.py to generate ndc_unii_rxnorm.json")
        subprocess.run([sys.executable, "ndc_unii.py"], check=True)
        steps.append("ndc_unii.py completed successfully")

        steps.append("Loading generated JSON output")
        with open("ndc_unii_rxnorm.json", encoding="utf-8") as f:
            data = json.load(f)
        steps.append(f"Loaded {len(data)} records from ndc_unii_rxnorm.json")
        data_counts = summarize_counts(data)
        steps.append(
            "Output dataset counts: " + html.escape(json.dumps(data_counts, sort_keys=True))
        )

        steps.append("Building expected dataset from RxNorm RRF files")
        expected = build_expected()
        steps.append(f"Built expected dataset with {len(expected)} records")
        expected_counts = summarize_counts(expected)
        steps.append(
            "Expected dataset counts: " + html.escape(json.dumps(expected_counts, sort_keys=True))
        )

        steps.append("Comparing script output to expected data")
        mismatches = []
        mismatch_counts = Counter()
        for datum, exp in zip(data, expected):
            if datum != exp:
                mismatches.append({"data": datum, "expected": exp})
                for key in {"ndc", "tty", "rxcui", "str", "ingredients"}:
                    if datum.get(key) != exp.get(key):
                        mismatch_counts[key] += 1

        compared = min(len(data), len(expected))
        steps.append(f"Compared {compared} records")

        if mismatches:
            steps.append(f"Found {len(mismatches)} mismatched records")
            if mismatch_counts:
                steps.append(
                    "Mismatch counts by field: "
                    + html.escape(json.dumps(dict(mismatch_counts), sort_keys=True))
                )
            pair = mismatches[0]
            datum, exp = pair["data"], pair["expected"]
            pytest.fail(
                "Record mismatch for NDC {d_ndc}/RxCUI {d_rxcui} vs NDC {e_ndc}/RxCUI {e_rxcui}\n"
                "data ingredients:\n{d_ing}\nexpected ingredients:\n{e_ing}\nfull diff:\n{diff}".format(
                    d_ndc=datum.get("ndc"),
                    d_rxcui=datum.get("rxcui"),
                    e_ndc=exp.get("ndc"),
                    e_rxcui=exp.get("rxcui"),
                    d_ing=json.dumps(datum.get("ingredients"), indent=2),
                    e_ing=json.dumps(exp.get("ingredients"), indent=2),
                    diff=json.dumps(pair, indent=2),
                )
            )

        if len(data) != len(expected):
            steps.append(
                f"Record count mismatch: {len(data)} records vs {len(expected)} expected"
            )
            pytest.fail(
                f"Length mismatch: {len(data)} records vs {len(expected)} expected",
            )

        steps.append(
            f"All records matched; output records: {len(data)}, expected records: {len(expected)}"
        )
    except Exception as exc:  # pragma: no cover - exception path
        steps.append(f"Test failed: {exc}")
        raise
    finally:
        with open("test_json_matches_rrf_report.html", "w", encoding="utf-8") as rep:
            rep.write(
                "<html><head><meta charset='utf-8'><title>test_json_matches_rrf Report"\
                "</title></head><body><h1>test_json_matches_rrf Report</h1><ul>"
            )
            for step in steps:
                rep.write(f"<li>{html.escape(str(step))}</li>")
            rep.write("</ul></body></html>")
