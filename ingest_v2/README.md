# ingest_v2: Boletín Oficial CABA → licitaciones

A prototype that replaces the PDF download and amount extraction in `scripts/boletin_oficial_api.py` and `scripts/transformaciones_licitaciones.py`.

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python ingest.py --days 7 --out out/        # the last 7 days
.venv/bin/python ingest.py --from 2026-11-12 --to 2026-11-19
.venv/bin/python llm_fallback.py out/licitaciones.jsonl --dry-run   # Claude Haiku batch for uncertain docs (needs `anthropic` + ANTHROPIC_API_KEY for --submit)
```

## Results on 12–19 Nov 2026 (269 licitaciones), compared with the current code

| | Current code | ingest_v2 |
|---|---|---|
| Wall time | ~21 min | 137 s (cached rerun: 0.7 s) |
| PDFs lost | 10 (saved as empty text) | 0 |
| Award docs with amount | 85% | 98% |
| Currency | none (USD counted as ARS) | ARS / USD / EUR per amount |

Details are in `results/`.

## What changed
- `ingest.py`: asyncio + httpx with concurrency 6, a minimum gap between requests, and retries with backoff. Rewrites http to https. Crawls by boletín number, because the API returns the latest boletín for dates without one and only one boletín per date. Keeps a raw cache keyed by `id_norma`, so nothing is downloaded twice. Fetches annexes for award stages. A failed download is recorded as an error, not as empty text.
- `parse_lic.py`: maps the stage (including second preadjudication, rescission and the like), the BAC process code, CUITs validated with the check digit, and the awarded company line. Extracts amounts with their currency and checks the total against the line items. Assigns a confidence level and the reason a document needs the LLM.
- `llm_fallback.py`: Claude Haiku via the Batch API with a JSON schema, and a check that each quoted snippet really appears in the text.
- Text is extracted with pypdfium2 (4 ms per PDF, compared with 38 ms for pdfplumber; permissive license).
- Evaluation scripts: `compare_old.py`, `eval_old_fuzzy.py` (the current fuzzy matching has precision 0.52 and recall 0.22), `bac_join.py`, `reparse.py`.

## Notes
- BAC `bac_anual.csv` is about 3 months behind and only keeps the first item of each list, so use it for enrichment (CUITs), not as the daily source. It joins on `tender/id`.
- For the history already in Postgres, re-parse the stored text and only re-download rows with empty text.
