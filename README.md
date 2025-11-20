# NDC to UNII

Identifies RXCUIs, ingredients, and UNII codes for all SAB=RXNORM NDCs in the RxNorm release files. The script fetches the Current Prescribable Content release from NLM, unzips it, builds the NDC mapping, and launches a local web interface.

Quick start:
- ``python ndc_unii.py``  
  - downloads `RxNorm_full_prescribe_current.zip` (Current Prescribable Content), extracts the RRF files, generates `ndc_unii_rxnorm.json`, builds chunked viewer data, and serves the UI.
- Skip automatic download (fail fast if files are missing): ``python ndc_unii.py --skip-download``
- Skip launching the web UI: ``python ndc_unii.py --no-web``
- Choose a different port: ``python ndc_unii.py --port 0`` (auto-pick) or any integer port

Run test_ndc_unii.py to validate the output. 

## Serverless HTML viewer

A lightweight static viewer lives in `web/`. It avoids loading the full JSON (hundreds of MB) by fetching small chunk files on demand and a tiny search index for name/RxCUI/UNII lookups.

- Build chunked data from the generated `ndc_unii_rxnorm.json`:
  - `python web/build_chunks.py`
- Serve the `web/` directory (any static server is fine), e.g. from repo root:
  - `python -m http.server 8080`
  - Open `http://localhost:8080/web/`

Usage: search by NDC (3+ digits), name, RxCUI, or UNII. The viewer pulls only the needed buckets and filters client-side. There are example chips under the search box you can click to see real queries from the dataset.

Scope note: this tool only includes NDCs present in the RxNorm Current Prescribable Content monthly release. For deeper or historical NDC data, see the RxNorm API.
