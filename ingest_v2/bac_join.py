"""Join parsed Boletín items with BAC OCDS (bac_anual.csv) on the BAC procedure code (tender/id)."""
import csv, json, sys, collections
csv.field_size_limit(10**8)
bac_path, jsonl = sys.argv[1], sys.argv[2]
tenders = collections.defaultdict(lambda: {"rows": 0, "suppliers": {}, "award_amounts": set(), "currencies": set(), "dates": set(), "tags": set()})
years = collections.Counter(); n = 0
with open(bac_path, newline="", encoding="utf-8", errors="replace") as fh:
    for r in csv.DictReader(fh):
        n += 1
        tid = (r.get("tender/id") or "").strip()
        years[(r.get("date") or "")[:4]] += 1
        t = tenders[tid]; t["rows"] += 1; t["tags"].add(r.get("tag"))
        sid, sname = r.get("awards/0/suppliers/0/id"), r.get("awards/0/suppliers/0/name")
        if sid: t["suppliers"][sid] = sname
        if r.get("awards/0/value/amount"):
            t["award_amounts"].add((r.get("awards/0/id"), float(r["awards/0/value/amount"]), r.get("awards/0/value/currency")))
        t["dates"].add((r.get("date") or "")[:10])
print("bac rows", n, "distinct tenders", len(tenders), "years", dict(years.most_common(8)))
rows = [json.loads(l) for l in open(jsonl)]
hit = collections.Counter(); ex = []
for r in rows:
    p = r["parsed"]; code = p["codigo_bac"]
    if not code: hit["no_code"] += 1; continue
    t = tenders.get(code)
    if not t: hit["code_not_in_bac"] += 1; continue
    hit["code_in_bac"] += 1
    if p["etapa"] in ("preadjudicacion", "adjudicacion"):
        hit["award_stage_in_bac"] += 1
        if t["award_amounts"]:
            hit["award_stage_with_bac_award"] += 1
            bsum = sum(a for _, a, _ in t["award_amounts"])
            mine = sum(p["totals"].values()) if p["totals"] else None
            ok = mine and abs(bsum - mine) < 0.02 * max(bsum, mine)
            hit["amount_match" if ok else "amount_differs"] += 1
            cuits_bo = {w["cuit"] for w in p["winners"] if w["cuit"]}
            if cuits_bo: hit["cuit_overlap" if cuits_bo & set(t["suppliers"]) else "cuit_no_overlap"] += 1
            if len(ex) < 6: ex.append({"code": code, "etapa": p["etapa"], "bo_totals": p["totals"], "bac_awards_sum": bsum, "bac_n_awards": len(t["award_amounts"]), "bac_suppliers": list(t["suppliers"].values())[:3], "bo_winners": [w["nombre"] for w in p["winners"]][:3]})
print(dict(hit)); print(json.dumps(ex, ensure_ascii=False, indent=1))
