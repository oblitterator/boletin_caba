"""Re-parse cached texts in out/licitaciones.jsonl without touching the network."""
import json, sys
from parse_lic import parse_item, AMOUNT_STAGES
p = sys.argv[1] if len(sys.argv) > 1 else "out/licitaciones.jsonl"
rows = [json.loads(l) for l in open(p)]
for r in rows:
    r["parsed"] = parse_item({**r}, r["text"])
with open(p, "w") as fh:
    for r in rows: fh.write(json.dumps(r, ensure_ascii=False) + "\n")
a = [r["parsed"] for r in rows if r["parsed"]["etapa"] in AMOUNT_STAGES]
print({"amount_stage": len(a), "amount_hit": sum(bool(x["totals"]) for x in a),
       "explicit_or_structured": sum(x["amount_method"] in ("kw_total","sum_winners","sum_items","kw_other") for x in a),
       "with_winner": sum(bool(x["winners"]) for x in a), "with_cuit": sum(any(w["cuit"] for w in x["winners"]) for x in a),
       "high": sum(x["confidence"]=="high" for x in a), "needs_llm": sum(x["needs_llm"] for x in a),
       "usd": sum("USD" in x["totals"] for x in a)})
import collections; print(collections.Counter(r for x in a for r in x["llm_reasons"]))
