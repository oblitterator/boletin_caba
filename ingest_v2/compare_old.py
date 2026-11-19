"""Apply the OLD repo parsing (extraer_campos_licitacion + extraer_monto_total) to the same texts and compare."""
import json, sys, collections
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import pandas as pd
from scripts.transformaciones_licitaciones import extraer_campos_licitacion, limpiar_texto_licitacion, extraer_monto_total
rows = [json.loads(l) for l in open(sys.argv[1])]
out = []
for r in rows:
    old = extraer_campos_licitacion(pd.Series({"nombre": r["nombre"], "sumario": r["sumario"]}))
    etapa_old = old["etapa_licitacion"]
    monto_old = extraer_monto_total(limpiar_texto_licitacion(r["text"])) if etapa_old in ("preadjudicacion", "adjudicacion", "prorroga") else 0
    p = r["parsed"]
    new_total = p["totals"]
    out.append({"id_norma": r["id_norma"], "etapa_old": etapa_old, "etapa_new": p["etapa"], "monto_old": monto_old,
                "totals_new": new_total, "method_new": p["amount_method"], "conf": p["confidence"]})
amount_new = {"preadjudicacion", "adjudicacion"}
sel = [o for o in out if o["etapa_new"] in amount_new]
print("items", len(out), "| stage unmapped old:", sum(o["etapa_old"] is None for o in out), "new:", sum(o["etapa_new"] is None for o in out))
print("old stage None examples:", collections.Counter(r["nombre"].split("/")[1].split("N°")[0].strip() for r, o in zip(rows, out) if o["etapa_old"] is None))
print("pre/adjudicacion items:", len(sel))
print("  old monto>0:", sum(o["monto_old"] > 0 for o in sel), " new totals:", sum(bool(o["totals_new"]) for o in sel))
agree = disagree = 0; dis = []
for o in sel:
    nv = list(o["totals_new"].values())
    if o["monto_old"] > 0 and nv:
        if any(abs(o["monto_old"] - v) < 0.01 * v for v in nv) and len(nv) == 1: agree += 1
        else: disagree += 1; dis.append(o)
print("  both found -> agree:", agree, "disagree:", disagree)
print("  USD docs (new):", sum("USD" in o["totals_new"] for o in sel), "- old code treats all as pesos")
print("  prorroga items where old code extracted an amount (spurious, prórroga = new opening date):", sum(o["etapa_old"]=="prorroga" and o["monto_old"]>0 for o in out))
json.dump({"rows": out, "disagreements": dis}, open(sys.argv[2], "w"), ensure_ascii=False, indent=1)
for o in dis: print("   DIS", o["id_norma"], o["monto_old"], o["totals_new"], o["method_new"])
