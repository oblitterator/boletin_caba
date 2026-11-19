"""Run the OLD fuzzy company matcher (procesar_licitaciones_con_progreso, threshold 85) on this week's
award-stage texts, with suppliers from bac_anual.csv, and compare with CUITs printed in the documents."""
import json, sys, time, logging
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
logging.disable(logging.CRITICAL)
import pandas as pd
from scripts.transformaciones_empresas import generar_df_empresas_ocid
from scripts.transformaciones_licitaciones import procesar_licitaciones_con_progreso
bac = pd.read_csv(sys.argv[1], usecols=lambda c: c.startswith("parties/0/"), dtype=str)
emp = generar_df_empresas_ocid(bac)
print("suppliers in registry:", len(emp))
rows = [json.loads(l) for l in open(sys.argv[2])]
aw = [r for r in rows if r["parsed"]["etapa"] in ("preadjudicacion", "adjudicacion")]
df = pd.DataFrame({"id_norma": [r["id_norma"] for r in aw], "texto_licitaciones": [r["text"] for r in aw]})
t0 = time.time(); res = procesar_licitaciones_con_progreso(df, emp); dt = time.time() - t0
fmt = lambda c: f"{c[:2]}-{c[2:10]}-{c[10]}"
tp = fp = fn = 0; docs_with_truth = 0; ex = []
for r, found in zip(aw, res):
    truth = {fmt(w["cuit"]) for w in r["parsed"]["winners"] if w["cuit"]}
    got = {e["cuit"] for e in found}
    if truth:
        docs_with_truth += 1
        tp += len(truth & got); fn += len(truth - got); fp += len(got - truth)
        if got - truth and len(ex) < 5: ex.append({"id": r["id_norma"], "truth": sorted(truth), "fuzzy": [e["nombre"] for e in found]})
print(json.dumps({"docs": len(aw), "seconds": round(dt, 1), "docs_with_cuit_in_text": docs_with_truth,
                  "tp": tp, "fp": fp, "fn": fn,
                  "precision": round(tp / max(tp + fp, 1), 2), "recall": round(tp / max(tp + fn, 1), 2),
                  "docs_with_any_fuzzy_match": sum(bool(x) for x in res), "fp_examples": ex}, ensure_ascii=False, indent=1))
