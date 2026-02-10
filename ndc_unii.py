#!/usr/bin/env python3
import argparse
import csv, json, sys, re, os, subprocess, webbrowser, functools, urllib.request, zipfile
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from collections import defaultdict

# --- Files (prefer extracted RxNorm_full_prescribe_current/rrf) ---
RRF_DIR  = Path("RxNorm_full_prescribe_current") / "rrf"
RXN_ZIP_URL = "https://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_current.zip"

def rrf_file(filename: str) -> Path:
    """Pick RRF file path from the preferred folder, fall back to CWD."""
    preferred = RRF_DIR / filename
    if preferred.is_file():
        return preferred
    fallback = Path(filename)
    if fallback.is_file():
        return fallback
    return preferred

RXNSAT   = rrf_file("RXNSAT.RRF")
RXNCONSO = rrf_file("RXNCONSO.RRF")
RXNREL   = rrf_file("RXNREL.RRF")
OUTPUT   = Path("ndc_unii_rxnorm.json")
ROOT     = Path(__file__).resolve().parent

def log(msg: str):
    print(f"[ndc_unii] {msg}", flush=True)

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Build NDC to UNII mapping from RxNorm RRF files.")
    parser.add_argument(
        "--no-web",
        action="store_true",
        help="Skip building/serving the web viewer after generating ndc_unii_rxnorm.json.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("NDC_UNII_WEB_PORT", "8080")),
        help="Port for the local web viewer (use 0 for an available ephemeral port).",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Do not attempt to download RxNorm; fail fast if RRF files are missing.",
    )
    return parser.parse_args(argv)

DIRECT_TTYS = {"SCD","SBD","GPCK","BPCK"}
ALL_TTYS    = {"SCD","SBD","GPCK","BPCK","SCDC","IN","PIN"}
# SCDCs that should be ignored when linking ingredients (known bad/phantom entries)
EXCLUDED_SCDC = {"1364431"}  # Apixaban SCDC incorrectly links via consists_of; drop the SCDC hop

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
    """Return maps for SCD -> SCDC -> ingredient target for RXN_AM/RXN_AI/RXN_BOSS_FROM."""
    am = defaultdict(dict)
    ai = defaultdict(dict)
    boss = defaultdict(dict)
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

            scd = row[0].strip()
            if not scd:
                continue

            # ATV contains "{SCDC_RxCUI} INGREDIENT_RxCUI"; parse both
            atv = row[10].strip()
            m = re.search(r"\{(\d+)\}\s*(\d+|AI|AM)", atv)
            if not m:
                continue
            scdc, target = m.group(1), m.group(2)
            if not scdc or not target:
                continue

            if atn == "RXN_AM":
                am[scd][scdc] = target
            elif atn == "RXN_AI":
                ai[scd][scdc] = target
            else:  # RXN_BOSS_FROM
                boss[scd][scdc] = target  # target may be RxCUI or 'AI'/'AM'
    return am, ai, boss

# ---------- RXNREL: hops needed to resolve to SCD and SCDC ingredients ----------
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

# ---------- Web helpers ----------
def build_web_chunks():
    log("Building web chunks (web/build_chunks.py)...")
    subprocess.run(
        [sys.executable, str(ROOT / "web" / "build_chunks.py")],
        check=True,
        cwd=ROOT,
    )

def serve_web(port: int):
    log("Starting local web server...")
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(ROOT))
    try:
        httpd = ThreadingHTTPServer(("localhost", port), handler)
    except OSError as exc:
        log(f"Could not start web server on port {port}: {exc}")
        log("Trying an available ephemeral port...")
        try:
            httpd = ThreadingHTTPServer(("localhost", 0), handler)
        except OSError as exc2:
            log(f"Could not start web server on any port: {exc2}")
            return
        log(f"Started web server on port {httpd.server_port} instead.")
    url = f"http://localhost:{httpd.server_port}/web/"
    log(f"Web viewer available at {url} (Ctrl+C to stop)")
    try:
        webbrowser.open(url, new=2)
    except Exception:
        pass
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log("Stopping web server")
    finally:
        httpd.server_close()

def ensure_rrf_files(skip_download: bool):
    missing = [p for p in (RXNSAT, RXNCONSO, RXNREL) if not p.is_file()]
    if not missing:
        return
    if skip_download:
        names = ", ".join(sorted({p.name for p in missing}))
        print(
            f"Missing RxNorm RRF file(s): {names}. Download "
            f"{RXN_ZIP_URL}, unzip it in the repo root, and ensure the RRF files exist under "
            "RxNorm_full_prescribe_current/rrf/ (or copy them to the current directory).",
            file=sys.stderr,
        )
        sys.exit(1)

    zip_path = ROOT / "RxNorm_full_prescribe_current.zip"
    log(f"Downloading RxNorm Current Prescribable Content to {zip_path} ...")
    try:
        urllib.request.urlretrieve(RXN_ZIP_URL, zip_path)
    except Exception as exc:
        print(f"Failed to download RxNorm zip: {exc}", file=sys.stderr)
        sys.exit(1)

    log("Extracting zip...")
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(ROOT)
    except Exception as exc:
        print(f"Failed to extract RxNorm zip: {exc}", file=sys.stderr)
        sys.exit(1)

    missing_after = [p for p in (RXNSAT, RXNCONSO, RXNREL) if not p.is_file()]
    if missing_after:
        names = ", ".join(sorted({p.name for p in missing_after}))
        print(
            f"After download, still missing: {names}. Verify zip contents.",
            file=sys.stderr,
        )
        sys.exit(1)
    log("RxNorm files ready.")

# ---------- Main ----------
def main(argv=None):
    args = parse_args(argv)
    ensure_rrf_files(args.skip_download)

    log("Loading RxNorm files...")
    tty_map, name_map, unii_map = load_conso(RXNCONSO)
    ndc_direct = load_ndc_direct(RXNSAT)
    sbd_to_scd, pack_to_scd, scd_to_scdc, scdc_to_in, scdc_to_pin = load_rel_maps(RXNREL, tty_map)
    if EXCLUDED_SCDC:
        # Remove known-bad SCDC hops before building ingredient links
        for scd, scdcs in list(scd_to_scdc.items()):
            filtered = {s for s in scdcs if s not in EXCLUDED_SCDC}
            if filtered:
                scd_to_scdc[scd] = filtered
            else:
                scd_to_scdc.pop(scd, None)
        for scdc in EXCLUDED_SCDC:
            scdc_to_in.pop(scdc, None)
            scdc_to_pin.pop(scdc, None)
    am_map, ai_map, boss_map = load_scd_attrs(RXNSAT)

    out = []

    log("Building NDC → ingredients mapping...")
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

            # Gather ingredients flagged directly on the SCD via RXN attributes
            ingredients = []
            seen_keys = set()
            seen_rxcuis = set()
            am_by_scdc   = am_map.get(scd, {})
            ai_by_scdc   = ai_map.get(scd, {})
            boss_by_scdc = boss_map.get(scd, {})

            for scdc in scd_to_scdc.get(scd, ()):  # include related ingredients via SCDC
                ai_target = ai_by_scdc.get(scdc)
                am_target = am_by_scdc.get(scdc)
                boss_key  = boss_by_scdc.get(scdc)
                if boss_key == "AI":
                    boss_target = ai_target
                elif boss_key == "AM":
                    boss_target = am_target
                else:
                    boss_target = boss_key

                for pin in scdc_to_pin.get(scdc, ()):  # precise ingredients
                    key = (scdc, pin)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    seen_rxcuis.add(pin)
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

                for inn in scdc_to_in.get(scdc, ()):  # ingredients
                    key = (scdc, inn)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    seen_rxcuis.add(inn)
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

            # Ensure attribute-linked ingredients that might lack SCDC rels are included
            ai_targets = {t for t in ai_by_scdc.values() if t and t.isdigit()}
            am_targets = {t for t in am_by_scdc.values() if t and t.isdigit()}
            basis_targets = set()
            for scdc, key in boss_by_scdc.items():
                if key == "AI":
                    target = ai_by_scdc.get(scdc)
                elif key == "AM":
                    target = am_by_scdc.get(scdc)
                else:
                    target = key
                if target and target.isdigit():
                    basis_targets.add(target)

            attr_targets = ai_targets | am_targets | basis_targets
            for target in sorted(attr_targets):
                if target in seen_rxcuis:
                    continue
                seen_rxcuis.add(target)
                ingredients.append({
                    "tty": tty_map.get(target, ""),
                    "rxcui": target,
                    "str": name_map.get(target, ""),
                    "unii": unii_map.get(target),
                    "active_ingredient": target in ai_targets,
                    "active_moiety": target in am_targets,
                    "basis_of_strength": target in basis_targets,
                    "scdc": None,
                })

            # Only keep rows that have at least one ingredient
            if not ingredients:
                continue

            # Sort ingredients deterministically by TTY then RxCUI for stable JSON
            ingredients.sort(key=lambda ing: (ing.get("tty") or "", ing["rxcui"]))

            out.append({
                "ndc": ndc,
                "tty": dtty,
                "rxcui": direct,
                "str": name_map.get(direct, ""),
                "ingredients": ingredients
            })

    # Ensure deterministic order of records
    out.sort(key=lambda rec: (rec["ndc"], rec["tty"], rec["rxcui"]))
    log("Writing ndc_unii_rxnorm.json...")
    with open(OUTPUT, "w", encoding="utf-8") as g:
        json.dump(out, g, indent=2)
    log(f"Wrote {len(out)} rows to {OUTPUT} ({len(out)} records)")

    if args.no_web:
        return

    build_web_chunks()
    serve_web(args.port)

if __name__ == "__main__":
    main()
