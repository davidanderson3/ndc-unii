import json
import subprocess
import sys

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


def test_json_matches_rrf():
    subprocess.run([sys.executable, "ndc_unii.py"], check=True)
    with open("ndc_unii_rxnorm.json", encoding="utf-8") as f:
        data = json.load(f)
    expected = build_expected()
    assert data == expected
