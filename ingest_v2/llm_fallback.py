"""Claude Haiku fallback via the Message Batches API for items the regex parser could not settle.

    python llm_fallback.py out/licitaciones.jsonl --dry-run     # writes out/llm_requests.jsonl, prints cost estimate
    python llm_fallback.py out/licitaciones.jsonl --submit      # needs Anthropic credentials (pip install anthropic)
    python llm_fallback.py --collect <batch_id>                 # writes out/llm_results.jsonl

Only rows with parsed.needs_llm are sent. custom_id = "<id_norma>-<text_sha256[:12]>" so results are
idempotent: the same text is never paid for twice, and a changed text gets a new id.
The regex result is passed as a hint; the model must quote the source text for every amount (evidence),
which lets us verify each value with a substring check before accepting it.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

MODEL = "claude-haiku-5-5"

SCHEMA = {
    "type": "object",
    "properties": {
        "etapa": {"type": "string", "enum": ["llamado", "apertura", "preadjudicacion", "adjudicacion", "prorroga",
                                             "circular", "ampliacion", "desierta_fracasada", "anulacion", "otra"]},
        "codigo_procedimiento": {"type": ["string", "null"], "description": "BAC code like 429-1574-LPU26 if present"},
        "unidad_contratante": {"type": ["string", "null"]},
        "adjudicatarios": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "nombre": {"type": "string"},
                    "cuit": {"type": ["string", "null"], "description": "11 digits, no dashes; null if not in text"},
                    "monto": {"type": ["number", "null"]},
                    "moneda": {"type": ["string", "null"], "enum": ["ARS", "USD", "EUR", None]},
                    "evidencia": {"type": "string", "description": "verbatim snippet of the text containing the amount"},
                },
                "required": ["nombre", "cuit", "monto", "moneda", "evidencia"],
                "additionalProperties": False,
            },
        },
        "totales": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"moneda": {"type": "string", "enum": ["ARS", "USD", "EUR"]},
                               "monto": {"type": "number"},
                               "evidencia": {"type": "string"}},
                "required": ["moneda", "monto", "evidencia"],
                "additionalProperties": False,
            },
        },
        "notas": {"type": ["string", "null"]},
    },
    "required": ["etapa", "codigo_procedimiento", "unidad_contratante", "adjudicatarios", "totales", "notas"],
    "additionalProperties": False,
}

SYSTEM = (
    "Extraés datos de publicaciones de licitaciones del Boletín Oficial de la Ciudad de Buenos Aires. "
    "Devolvé solo lo que dice el texto. Montos: el monto preadjudicado/adjudicado a cada firma, no el presupuesto "
    "oficial, ni garantías, ni precios unitarios, ni ofertas desestimadas. Si hay montos en dólares y su equivalente "
    "en pesos, informá el monto en la moneda de la adjudicación. Números en formato argentino (1.234.567,89). "
    "CUIT: solo si figura en el texto. 'evidencia' debe ser una cita textual del documento."
)


def build_requests(rows: list[dict]) -> list[dict]:
    reqs = []
    for r in rows:
        p = r.get("parsed") or {}
        if not p.get("needs_llm") or not r.get("text"):
            continue
        hint = {k: p.get(k) for k in ("etapa", "codigo_bac", "totals", "winners", "llm_reasons")}
        reqs.append({
            "custom_id": f"{r['id_norma']}-{(r.get('text_sha256') or '')[:12]}",
            "params": {
                "model": MODEL,
                "max_tokens": 2048,
                "system": SYSTEM,
                "output_config": {"effort": "low", "format": {"type": "json_schema", "schema": SCHEMA}},
                "messages": [{"role": "user", "content":
                              f"Título: {r.get('nombre')}\nOrganismo: {r.get('organismo')}\n"
                              f"Resultado preliminar (puede estar mal): {json.dumps(hint, ensure_ascii=False)}\n\n"
                              f"<documento>\n{r['text']}\n</documento>"}],
            },
        })
    return reqs


def verify(result: dict, text: str) -> bool:
    """Accept only amounts whose evidence snippet really appears in the source text."""
    norm = " ".join(text.split())
    snippets = [a["evidencia"] for a in result.get("adjudicatarios", [])] + [t["evidencia"] for t in result.get("totales", [])]
    return all(" ".join(s.split())[:60] in norm for s in snippets if s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl", nargs="?")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--submit", action="store_true")
    ap.add_argument("--collect")
    a = ap.parse_args()
    if a.collect:
        import anthropic
        client = anthropic.Anthropic()
        with open("out/llm_results.jsonl", "w") as fh:
            for res in client.messages.batches.results(a.collect):
                if res.result.type == "succeeded":
                    msg = res.result.message
                    text = next((b.text for b in msg.content if b.type == "text"), "")
                    fh.write(json.dumps({"custom_id": res.custom_id, "data": json.loads(text)}, ensure_ascii=False) + "\n")
                else:
                    fh.write(json.dumps({"custom_id": res.custom_id, "error": res.result.type}) + "\n")
        return
    rows = [json.loads(l) for l in open(a.jsonl)]
    reqs = build_requests(rows)
    out = Path(a.jsonl).parent / "llm_requests.jsonl"
    with open(out, "w") as fh:
        for q in reqs:
            fh.write(json.dumps(q, ensure_ascii=False) + "\n")
    chars = sum(len(q["params"]["messages"][0]["content"]) + len(SYSTEM) + len(json.dumps(SCHEMA)) for q in reqs)
    in_tok = chars / 3.2  # Spanish text ~3.2 chars/token (rough; use count_tokens for a real number)
    out_tok = 350 * len(reqs)
    # Haiku 5.5: $0.10 / $0.50 per MTok, Batch API -50%
    cost = (in_tok * 0.10 + out_tok * 0.50) / 1e6 * 0.5
    print(json.dumps({"requests": len(reqs), "est_input_tokens": int(in_tok), "est_output_tokens": out_tok,
                      "est_cost_usd": round(cost, 4), "file": str(out)}, indent=1))
    if a.submit:
        import anthropic
        client = anthropic.Anthropic()
        batch = client.messages.batches.create(requests=reqs)
        print("batch id:", batch.id, batch.processing_status)


if __name__ == "__main__":
    sys.exit(main())
