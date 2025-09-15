# NDC to UNII

Identifies RXCUIs, ingredients, and UNII codes for all SAB=RXNORM NDCs in the RxNorm release files. 

Add RXNCONSO.RRF, RXNREL.RRF, and RXNSAT.RRF to the root directory. 

Run ndc_unii.py to generate the file.

Run test_ndc_unii.py to validate the output. 

## Serverless HTML viewer

A lightweight static viewer lives in `web/`. It avoids loading the full JSON (hundreds of MB) by fetching small chunk files on demand.

- Build chunked data from the generated `ndc_unii_rxnorm.json`:
  - `python web/build_chunks.py`
- Serve the `web/` directory (any static server is fine), e.g. from repo root:
  - `python -m http.server 8080`
  - Open `http://localhost:8080/web/`

Usage: type at least 3 NDC digits to load the corresponding bucket; results filter client-side. You can also search within loaded results by name or UNII.
