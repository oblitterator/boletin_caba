"""Deterministic parsing of Boletín Oficial CABA "Licitaciones" items.

Pure functions, no I/O. Input: the API record (nombre, sumario, organismo...) and
the PDF text. Output: a dict with stage, procedure code, winners (CUIT, name,
amount, currency), totals per currency, a confidence level and `needs_llm`.
"""
from __future__ import annotations

import re
import unicodedata

# --------------------------------------------------------------------------- utils

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def norm_ws(s: str) -> str:
    # soft hyphen / NBSP / odd dashes that pdf text carries
    s = s.replace("­", " ").replace(" ", " ").replace("–", "-").replace("—", "-")
    s = re.sub(r"[ \t]+", " ", s)
    # kerning artefact: "$ 2.287.41 0,00" -> "$ 2.287.410,00"
    return SPLIT_NUM.sub(lambda m: m.group(1) + m.group(2) + m.group(3) if len(m.group(2) + m.group(3)) == 3 else m.group(0), s)


SPLIT_NUM = re.compile(r"(\d{1,3}(?:\.\d{3})*\.)(\d{1,2}) (\d{1,2})(?=,\d{2})")

# --------------------------------------------------------------------------- name / stage

STAGE_RULES = [  # order matters: first match wins
    (r"segunda preadjudic|preadjudic|pre adjudic", "preadjudicacion"),
    (r"desadjudic", "desadjudicacion"),
    (r"adjudic", "adjudicacion"),
    (r"preselec", "preseleccion"),
    (r"prorroga|posterga", "prorroga"),
    (r"circular con consulta", "circular_con_consulta"),
    (r"circular sin consulta|circular", "circular_sin_consulta"),
    (r"desiert|fracas", "desierta_fracasada"),
    (r"deja sin efecto|anulac|revoca|suspens", "anulacion"),
    (r"ampliac", "ampliacion"),
    (r"correcc|rectific|fe de erratas|modificac", "correccion"),
    (r"apertura", "apertura"),
    (r"llamado|convoca", "llamado"),
]

AMOUNT_STAGES = {"preadjudicacion", "adjudicacion", "ampliacion", "desadjudicacion"}


def normalize_stage(raw: str | None) -> str | None:
    if not raw:
        return None
    s = strip_accents(raw).lower()
    for pat, val in STAGE_RULES:
        if re.search(pat, s):
            return val
    return re.sub(r"\W+", "_", s).strip("_") or None


def parse_nombre(nombre: str | None) -> dict:
    """'Licitación Pública / Preadjudicación  N° 1204/DGAYCON/26' -> tipo, etapa, numero, reparticion, anio."""
    out = {"tipo": None, "etapa_raw": None, "numero_bo": None, "reparticion": None, "anio2": None}
    if not isinstance(nombre, str) or "/" not in nombre:
        return out
    tipo, rest = [p.strip() for p in nombre.split("/", 1)]
    out["tipo"] = tipo
    m = re.search(r"^(.*?)\s*N\s*[°ºo\.]\s*(.+)$", rest, re.I)  # N°, Nº, No.
    if m:
        out["etapa_raw"] = m.group(1).strip()
        code = m.group(2).strip()
        parts = code.split("/")
        out["numero_bo"] = parts[0]
        if len(parts) >= 3:
            out["reparticion"], out["anio2"] = parts[1], parts[-1]
    else:
        out["etapa_raw"] = rest
    return out

# --------------------------------------------------------------------------- procedure code (BAC)

BAC_RE = re.compile(r"\b(\d{2,5})\s*-\s*(\d{1,5})\s*-\s*(LPU|LPR|CME|CDI|LOP|CDE|CPU|CPR|SIE|LPI|CEX|LPUAB|[A-Z]{3})\s*(\d{2})\b")


def bac_codes(text: str) -> list[str]:
    seen = []
    for uoa, num, kind, yy in BAC_RE.findall(text):
        code = f"{int(uoa)}-{int(num):04d}-{kind}{yy}"  # BAC/OCDS tender/id pads the number to 4
        if code not in seen:
            seen.append(code)
    return seen

# --------------------------------------------------------------------------- CUIT

CUIT_RE = re.compile(r"(?<![\d.])((?:20|23|24|25|26|27|30|33|34))\s*[-.]?\s*(\d{2}\.?\d{3}\.?\d{3})\s*[-.]?\s*(\d)(?![\d])")


def cuit_ok(c: str) -> bool:
    if len(c) != 11 or not c.isdigit():
        return False
    w = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2]
    s = sum(int(a) * b for a, b in zip(c[:10], w))
    dv = 11 - s % 11
    dv = 0 if dv == 11 else (9 if dv == 10 else dv)
    return dv == int(c[10])


def find_cuits(text: str) -> list[tuple[int, str]]:
    out = []
    for m in CUIT_RE.finditer(text):
        c = (m.group(1) + m.group(2) + m.group(3)).replace(".", "")
        if cuit_ok(c):
            out.append((m.start(), c))
    return out


NAME_NOISE = re.compile(
    r"(firma[s/]*\s*(pre)?adjudicad[ao][s/]*|oferta\s*(pre)?adjudicada|empresa|proveedor|oferente|raz[oó]n social"
    r"|c\.?\s*u\.?\s*i\.?\s*t\.?|n[°º]|renglon(es)?[^-:]*|opcion\s*\d+|:)",
    re.I,
)


def name_before(text: str, pos: int) -> str | None:
    line_start = text.rfind("\n", 0, pos) + 1
    seg = text[line_start:pos]
    if len(seg.strip(" -:(,.")) < 3:  # CUIT at line start: take previous line
        prev_start = text.rfind("\n", 0, max(line_start - 1, 0)) + 1
        seg = text[prev_start:line_start]
    seg = re.split(r"(?i)\bcuit\b|c\.u\.i\.t", seg)[0] if re.search(r"(?i)\bcuit\b|c\.u\.i\.t", seg) else seg
    seg = NAME_NOISE.sub(" ", seg)
    seg = re.sub(r"[\(\[\-–,;:]+\s*$", "", seg.strip()).strip(" -:(,.;")
    seg = re.sub(r"^.*?-\s*(?=[A-ZÁÉÍÓÚÑ][\w&.]+(\s|$))", "", seg) if seg.count("-") > 1 else seg
    seg = re.sub(r"\s+", " ", seg).strip(" -:(,.;")
    return seg[:120] or None

FIRMA_RE = re.compile(
    r"(?:firma|empresa|oferta|oferente|proveedor)(?:\s*\(?s\)?|s)?\s*(?:/s)?\s*(?:pre\s?)?adjudicad[ao](?:\s*\(?s\)?|s)?(?:/s)?\s*:?\s*\n?\s*"
    r"(?P<name>[^\n]{3,120})",
    re.I,
)


def firm_names(text: str) -> list[str]:
    """'Firma preadjudicada: Rayos X Dinan S.A.' (name may be on the next line)."""
    out = []
    for m in FIRMA_RE.finditer(text):
        n = m.group("name")
        n = re.split(r"(?i)\s*(\(|,\s*c\.?u\.?i\.?t|\bcuit\b|\s-\s|por\s+(un|una|la|el)\s|renglon|rengl[oó]n|$)", n)[0]
        n = re.sub(r"(?i)^.*?adjudicad[ao]s?\s*:\s*", "", n).strip(" .:-")
        for part in re.split(r"\s*;\s*", n):
            part = part.strip(" .:-;")
            if (len(part) >= 3 and part[0].isupper() and not re.match(r"(?i)(documento|de|del|la|el|en|a)\b", part)
                    and part not in out):
                out.append(part)
    return out

# --------------------------------------------------------------------------- amounts

CUR = r"(?P<cur>(?:U\s?\$\s?[SD]|US\s?\$|USD|u\$s)\.?|EUR|€|\$)"
NUM = r"(?P<num>\d{1,3}(?:\.\d{3})+(?:,\d{1,2})?|\d+,\d{1,2}|\d{4,})"
AMT_RE = re.compile(CUR + r"\s*:?\s*" + NUM + r"(?![\d])(?P<bad>,\d)?")
WORD_USD = re.compile(r"d[oó]lar(es)?(\s+estadounidenses)?", re.I)

TOTAL_KW = re.compile(
    r"((?m:^\s*total(\s+general)?\s*:)|total\s*,?\s*(de\s+la\s+)?(presente\s+)?(pre\s?)?adjudica(do|da|ci[oó]n)"
    r"|monto\s+(total\s+)?(de\s+la\s+)?(pre\s?)?adjudica(do|ci[oó]n)"
    r"|precio\s+total\s+de\s+la\s+pre\s?adjudicaci[oó]n(\s+final)?"
    r"|importe\s+total(\s+(pre)?adjudicado)?|monto\s+total|total\s+general"
    r"|erogaci[oó]n\s+total|asciende\s+a(\s+la\s+suma)?|por\s+(una|la)\s+suma(\s+total)?(\s+de)?"
    r"|suma\s+total|total\s*:?\s*(de\s+)?(pesos|d[oó]lares)|son\s+(pesos|d[oó]lares))",
    re.I,
)
GLOBAL_KW = re.compile(r"total|erogaci|monto\s+(total\s+)?de\s+la", re.I)
ITEM_TOTAL_KW = re.compile(r"(precio|p\.|importe|monto)\s*total|total\s*rengl", re.I)


def to_float(num: str) -> float | None:
    s = num
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return None


def cur_code(tok: str) -> str:
    t = tok.upper().replace(" ", "")
    if t in ("$",):
        return "ARS"
    if "EUR" in t or "€" in t:
        return "EUR"
    return "USD"


def find_amounts(text: str) -> list[dict]:
    out = []
    for m in AMT_RE.finditer(text):
        v = to_float(m.group("num"))
        if v is None:
            continue
        cur = cur_code(m.group("cur"))
        out.append({"pos": m.start(), "end": m.end(), "value": v, "currency": cur, "malformed": bool(m.group("bad"))})
    return out


REFERENCE_CTX = re.compile(r"presupuest|partida|garant[ií]a|justiprecio|valor\s+estimado|monto\s+estimado|pliego", re.I)


def is_item_total(text: str, a: dict) -> bool:
    """Amount is a line-item total: 'precio total: $X' on the same line, or 'precio total:' ending the previous line."""
    line_s = text.rfind("\n", 0, a["pos"]) + 1
    same = text[line_s: a["pos"]]
    if ITEM_TOTAL_KW.search(same[-60:]):
        return True
    prev = text[max(0, line_s - 40): line_s]
    return bool(re.search(r"(precio|importe)\s*total\s*:?\s*\n$", prev, re.I)) and not same.strip(" $USD")


def amounts_after_keywords(text: str, amounts: list[dict], window: int = 260) -> list[dict]:
    """Amount that follows a 'total' keyword (first one within `window` chars)."""
    hits = []
    for km in TOTAL_KW.finditer(text):
        cands = [a for a in amounts if km.end() <= a["pos"] <= km.end() + window]
        if not cands:
            continue
        a = dict(cands[0])
        # "DÓLARES ... (USD x)" already tagged; "$" preceded by 'dólares' words means USD
        ctx = text[km.start(): a["pos"]]
        if a["currency"] == "ARS" and WORD_USD.search(ctx) and not re.search(r"pesos", ctx, re.I):
            a["currency"] = "USD"
        a["kw"] = km.group(0).lower()
        a["kw_pos"] = km.start()
        a["global"] = bool(GLOBAL_KW.search(km.group(0)))
        if REFERENCE_CTX.search(text[max(0, km.start() - 220): a["pos"]]):
            continue  # 'presupuesto oficial ... por un total de $X' is a budget, not an award
        hits.append(a)
    return hits

# --------------------------------------------------------------------------- main entry


def parse_item(rec: dict, text: str) -> dict:
    nom = parse_nombre(rec.get("nombre"))
    stage = normalize_stage(nom["etapa_raw"])
    if not stage:  # fallback on sumario / first lines of text
        stage = normalize_stage(rec.get("sumario") or "") or normalize_stage(text[:600])
    t = norm_ws(text or "")
    codes = bac_codes(t)
    res = {
        "id_norma": rec.get("id_norma"),
        "fecha": rec.get("fecha"),
        "tipo_procedimiento": nom["tipo"],
        "etapa_raw": nom["etapa_raw"],
        "etapa": stage,
        "numero_bo": nom["numero_bo"],
        "reparticion": nom["reparticion"],
        "organismo": rec.get("organismo"),
        "codigo_bac": codes[0] if codes else None,
        "codigos_bac_all": codes,
        "winners": [],
        "totals": {},
        "amount_method": None,
        "confidence": None,
        "needs_llm": False,
        "llm_reasons": [],
        "currencies_seen": [],
    }
    if len(t.strip()) < 50:
        res["needs_llm"] = False
        res["llm_reasons"].append("no_text")
        res["confidence"] = "none"
        return res

    amounts = find_amounts(t)
    res["currencies_seen"] = sorted({a["currency"] for a in amounts})
    if stage not in AMOUNT_STAGES:
        res["amount_method"] = "n/a_stage"
        return res

    kw = amounts_after_keywords(t, amounts)
    cuits = find_cuits(t)

    # ---- winners: segment text by first occurrence of each distinct CUIT
    firsts, seen = [], set()
    for pos, c in cuits:
        if c not in seen:
            seen.add(c)
            firsts.append((pos, c))
    winners = []
    for i, (pos, c) in enumerate(firsts):
        seg_start = t.rfind("\n", 0, pos) + 1
        seg_end = firsts[i + 1][0] if i + 1 < len(firsts) else len(t)
        seg_end = t.rfind("\n", 0, seg_end) + 1 if i + 1 < len(firsts) else seg_end
        seg_amts = [a for a in amounts if seg_start <= a["pos"] < seg_end]
        seg_kw = [a for a in kw if seg_start <= a["kw_pos"] < seg_end]
        amt, cur, how = None, None, None
        if seg_kw:
            a = seg_kw[0]; amt, cur, how = a["value"], a["currency"], "kw"
        else:
            items = [a for a in seg_amts if is_item_total(t, a)]
            if items:
                curs = {a["currency"] for a in items}
                cur = items[0]["currency"] if len(curs) == 1 else "MIXED"
                amt = round(sum(a["value"] for a in items), 2) if cur != "MIXED" else None
                how = "sum_items"
        winners.append({"cuit": c, "nombre": name_before(t, pos), "monto": amt, "moneda": cur, "metodo": how})
    if not winners:  # many hospital dictamens name the firm without CUIT
        winners = [{"cuit": None, "nombre": n, "monto": None, "moneda": None, "metodo": "name_only"} for n in firm_names(t)]
    res["winners"] = winners

    # ---- document totals per currency
    totals: dict[str, float] = {}
    method = None
    globals_ = [a for a in kw if a["global"]]
    if globals_:
        # last explicit 'total' per currency wins (dictamens often restate it at the end)
        for a in globals_:
            totals[a["currency"]] = a["value"]
        method = "kw_total"
        # several winners, each with its own amount: the 'last total' is usually one firm's subtotal
        ws = [w for w in winners if w["monto"] is not None and w["moneda"] not in (None, "MIXED")]
        if len(winners) > 1 and len(ws) == len(winners) and len({w["moneda"] for w in ws}) == 1:
            wsum = round(sum(w["monto"] for w in ws), 2)
            if not any(abs(a["value"] - wsum) < 0.01 * wsum for a in globals_):
                totals, method = {ws[0]["moneda"]: wsum}, "sum_winners"
    elif any(w["monto"] for w in winners):
        for w in winners:
            if w["monto"] is not None and w["moneda"] not in (None, "MIXED"):
                totals[w["moneda"]] = round(totals.get(w["moneda"], 0) + w["monto"], 2)
        method = "sum_winners"
    else:
        items = [a for a in amounts if is_item_total(t, a)]
        for a in items:
            totals[a["currency"]] = round(totals.get(a["currency"], 0) + a["value"], 2)
        method = "sum_items" if items else None
        if not totals and kw:  # 'por la suma de $X' etc. without an explicit 'total'
            for a in kw:
                totals[a["currency"]] = a["value"]
            method = "kw_other"
        if not totals and amounts:  # last resort, flagged low confidence (old code's heuristic)
            a = max(amounts, key=lambda x: x["value"])
            totals, method = {a["currency"]: a["value"]}, "max_amount"

    res["totals"] = totals
    res["amount_method"] = method

    # ---- confidence and LLM routing
    reasons = []
    if not totals:
        reasons.append("no_amount")
    if method == "max_amount":
        reasons.append("weak_amount_heuristic")
    if not winners:
        reasons.append("no_winner")
    if len(winners) == 1 and winners[0]["monto"] is None and len(totals) == 1:
        (cur, val), = totals.items()
        winners[0].update(monto=val, moneda=cur, metodo="single_winner_total")
    if winners and all(w["cuit"] is None for w in winners):
        res["needs_registry_match"] = True  # resolve CUIT via BAC supplier registry, not the LLM
    if any(w["monto"] is None for w in winners) and len(winners) > 1:
        reasons.append("winner_without_amount")
    if len(totals) > 1:
        reasons.append("multi_currency")
    # cross-check: explicit total vs sum of line items (same currency); catches typos and per-lot totals
    items = [a for a in amounts if is_item_total(t, a)]
    if method in ("kw_total", "kw_other") and items and len(totals) == 1:
        (cur, val), = totals.items()
        isum = sum(a["value"] for a in items if a["currency"] == cur)
        if isum and abs(isum - val) > 0.01 * max(val, isum) and not any(abs(a["value"] - val) < 0.01 * val for a in items):
            reasons.append("items_total_mismatch")
    if any(x.get("malformed") for x in amounts):
        reasons.append("malformed_number")
    if totals and len(winners) == 1 and winners[0]["metodo"] == "kw" and len(totals) == 1:
        if abs(winners[0]["monto"] - next(iter(totals.values()))) > 0.01 * winners[0]["monto"]:
            reasons.append("winners_total_mismatch")
    if totals and winners and method == "kw_total" and len(winners) > 1:
        wsum = sum(w["monto"] or 0 for w in winners if w["moneda"] in totals)
        if wsum and abs(wsum - sum(totals.values())) > 0.01 * sum(totals.values()):
            reasons.append("winners_total_mismatch")
    res["llm_reasons"] = reasons
    res["needs_llm"] = bool(reasons)
    if not reasons:
        res["confidence"] = "high"
    elif totals:
        res["confidence"] = "medium"
    else:
        res["confidence"] = "low"
    return res
