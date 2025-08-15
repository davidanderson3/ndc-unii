#!/usr/bin/env python3
import csv, json, sys
from pathlib import Path
from collections import defaultdict

# --- Files (must be in current dir) ---
RXNSAT   = Path("RXNSAT.RRF")
RXNCONSO = Path("RXNCONSO.RRF")
RXNREL   = Path("RXNREL.RRF")
OUTPUT   = Path("ndc_unii_rxnorm.json")

DIRECT_TTYS = {"SCD","SBD","GPCK","BPCK"}
ALL_TTYS    = {"SCD","SBD","GPCK","BPCK","SCDC","IN","PIN"}

# ---------- Load CONSO (TTYs, names, UNIIs) ----------
def load_conso(p):
    """
    Build:
      tty[rxcui]   -> TTY in ALL_TTYS          (0-based: SAB=11, TTY=12)
      name[rxcui]  -> best STR (SAB=RXNORM; prefer TS='P'&ISPREF='Y' > ISPREF='Y' > TS='P' > any)
                      (TS=2, ISPREF=6, STR=14)
      unii[rxcui]  -> CODE where SAB='MTHSPL' and TTY='SU' (CODE=13)
    """
    tty, name, best, unii = {}, {}, {}, {}
    with open(p, newline="", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="|")
        for row in r:
            if len(row) < 15: 
                continue
            rxcui = row[0].strip()
            ts    = row[2].strip()
            ispref= row[6].strip()
            sab   = row[11].strip()
            t     = row[12].strip()
            code  = row[13].strip()
            s     = row[14].strip()

            if sab == "RXNORM" and t in ALL_TTYS:
                tty[rxcui] = t
                score = 0 if (ts=="P" and ispref=="Y") else 1 if ispref=="Y" else 2 if ts=="P" else 3
                if rxcui not in best or score < best[rxcui]:
                    best[rxcui] = score
                    name[rxcui] = s

            if sab == "MTHSPL" and t == "SU" and code:
                unii.setdefault(rxcui, code)
    return tty, name, unii

# ---------- RXNSAT: direct NDC→RXCUI ----------
def load_ndc_direct(rxnsat_path):
    """Return dict ndc -> set(direct RXCUIs) for ATN='NDC' & SAB='RXNORM' & SUPPRESS='N'."""
    ndc_to_rxcuis = defaultdict(set)
    with open(rxnsat_path, newline="", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="|")
        for row in r:
            if len(row) < 12: 
                continue
            if row[8].strip()!="NDC" or row[9].strip()!="RXNORM" or row[11].strip()!="N":
                continue
            ndc   = row[10].strip()
            rxcui = row[0].strip()
            ndc_to_rxcuis[ndc].add(rxcui)
    return ndc_to_rxcuis

# ---------- RXNSAT: active ingredient/moiety/basis of strength ----------
def load_scd_attrs(rxnsat_path):
    """Return maps for SCD -> SCDC -> ingredient rxcui for RXN_AM, RXN_AI and RXN_BOSS_FROM."""
    am  = defaultdict(dict)  # active moiety (IN)
    ai  = defaultdict(dict)  # active ingredient (PIN)
    boss= defaultdict(dict)  # basis of strength substance
    with open(rxnsat_path, newline="", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="|")
        for row in r:
            if len(row) < 12:
                continue
            atn = row[8].strip()
            if atn not in {"RXN_AM","RXN_AI","RXN_BOSS_FROM"}:
                continue
            if row[9].strip() != "RXNORM":
                continue
            scd   = row[0].strip()
            scdc  = row[5].strip()
            target= row[10].strip()
            if not scd or not scdc or not target:
                continue
            if atn == "RXN_AM":
                am[scd][scdc] = target
            elif atn == "RXN_AI":
                ai[scd][scdc] = target
            else:  # RXN_BOSS_FROM
                boss[scd][scdc] = target
    return am, ai, boss

# ---------- RXNREL: hops (bidirectional where needed) ----------
def load_rel_maps(rel_path, tty_map):
    """
    Build maps:
      SBD <-> SCD via 'tradename_of'           -> sbd_to_scd
      SCD <-> GPCK/BPCK via 'contains'         -> pack_to_scd (child pack -> SCD)
      SCD <-> SCDC via 'consists_of'           -> scd_to_scdc
      SCDC <-> IN via 'has_ingredient'         -> scdc_to_in
      SCDC <-> PIN via 'has_precise_ingredient'-> scdc_to_pin
    """
    sbd_to_scd  = {}
    pack_to_scd = {}
    scd_to_scdc = defaultdict(set)
    scdc_to_in  = defaultdict(set)
    scdc_to_pin = defaultdict(set)

    with open(rel_path, newline="", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="|")
        for row in r:
            if len(row) < 11: 
                continue
            c1, c2, rela, sab = row[0].strip(), row[4].strip(), row[7].strip(), row[10].strip()
            if sab != "RXNORM":
                continue
            t1, t2 = tty_map.get(c1), tty_map.get(c2)

            if rela == "tradename_of":
                if t1=="SBD" and t2=="SCD": sbd_to_scd.setdefault(c1, c2)
                if t2=="SBD" and t1=="SCD": sbd_to_scd.setdefault(c2, c1)

            elif rela == "contains":
                if t1=="SCD" and t2 in {"GPCK","BPCK"}: pack_to_scd.setdefault(c2, c1)
                if t2=="SCD" and t1 in {"GPCK","BPCK"}: pack_to_scd.setdefault(c1, c2)

            elif rela == "consists_of":
                if t1=="SCD" and t2=="SCDC": scd_to_scdc[c1].add(c2)
                if t2=="SCD" and t1=="SCDC": scd_to_scdc[c2].add(c1)

            elif rela == "has_ingredient":
                if t1=="SCDC" and t2=="IN": scdc_to_in[c1].add(c2)
                if t2=="SCDC" and t1=="IN": scdc_to_in[c2].add(c1)

            elif rela == "has_precise_ingredient":
                if t1=="SCDC" and t2=="PIN": scdc_to_pin[c1].add(c2)
                if t2=="SCDC" and t1=="PIN": scdc_to_pin[c2].add(c1)

    return sbd_to_scd, pack_to_scd, scd_to_scdc, scdc_to_in, scdc_to_pin

# ---------- Main ----------
def main():
    for p in (RXNSAT, RXNCONSO, RXNREL):
        if not p.is_file():
            print(f"Missing {p.name} in {Path.cwd()}", file=sys.stderr)
            sys.exit(1)

    tty_map, name_map, unii_map = load_conso(RXNCONSO)
    ndc_direct = load_ndc_direct(RXNSAT)
    sbd_to_scd, pack_to_scd, scd_to_scdc, scdc_to_in, scdc_to_pin = load_rel_maps(RXNREL, tty_map)
    am_map, ai_map, boss_map = load_scd_attrs(RXNSAT)

    out = []

    for ndc, direct_rxcuis in ndc_direct.items():
        # one record PER direct attachment
        for direct in sorted(direct_rxcuis):
            dtty = tty_map.get(direct)
            if dtty not in DIRECT_TTYS:
                continue

            # resolve SCD for ingredient traversal
            scd = None
            if dtty == "SCD":
                scd = direct
            elif dtty == "SBD":
                scd = sbd_to_scd.get(direct)
            elif dtty in {"GPCK","BPCK"}:
                scd = pack_to_scd.get(direct)

            if not scd:  # skip if no SCD resolution (should be rare)
                continue

            # SCD -> SCDC -> IN/PIN (could be multiple)
            ingredients = []
            seen_ing = set()
            am_by_scdc   = am_map.get(scd, {})
            ai_by_scdc   = ai_map.get(scd, {})
            boss_by_scdc = boss_map.get(scd, {})
            for scdc in scd_to_scdc.get(scd, ()):
                ai_target   = ai_by_scdc.get(scdc)
                am_target   = am_by_scdc.get(scdc)
                boss_target = boss_by_scdc.get(scdc)
                # PINs
                for pin in scdc_to_pin.get(scdc, ()):
                    key = (scdc, pin)
                    if key in seen_ing:
                        continue
                    seen_ing.add(key)
                    ingredients.append({
                        "scdc": scdc,
                        "tty": "PIN",
                        "rxcui": pin,
                        "str": name_map.get(pin, ""),
                        "unii": unii_map.get(pin),
                        "active_ingredient": pin == ai_target,
                        "basis_of_strength": pin == boss_target
                    })
                # INs
                for inn in scdc_to_in.get(scdc, ()):
                    key = (scdc, inn)
                    if key in seen_ing:
                        continue
                    seen_ing.add(key)
                    ingredients.append({
                        "scdc": scdc,
                        "tty": "IN",
                        "rxcui": inn,
                        "str": name_map.get(inn, ""),
                        "unii": unii_map.get(inn),
                        "active_moiety": inn == am_target,
                        "basis_of_strength": inn == boss_target
                    })

            # Only keep rows that have at least one ingredient
            if not ingredients:
                continue

            out.append({
                "ndc": ndc,
                "tty": dtty,
                "rxcui": direct,
                "str": name_map.get(direct, ""),
                "ingredients": ingredients
            })

    with open(OUTPUT, "w", encoding="utf-8") as g:
        json.dump(out, g, indent=2)
    print(f"Wrote {len(out)} rows to {OUTPUT}")

if __name__ == "__main__":
    main()
