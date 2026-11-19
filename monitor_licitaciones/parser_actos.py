"""
Parser por reglas de actos de licitación del Boletín Oficial CABA.

Extrae de cada acto (llamado, preadjudicación, adjudicación, ...):
  - código de proceso BAC (p.ej. 401-0147-LPU22) o formato viejo (5072/SIGAF/20)
  - etapa (desde el título del texto; el nombre de la norma es respaldo)
  - monto total, cantidad de ofertas y si hubo oferta única
  - firmas con su rol (ganadora / oferente / desestimada), CUIT si figura y monto si
    el texto lo asigna a esa firma

Principios:
  - Nunca inventa: si un campo no se encuentra queda en None y se lista en `faltantes`.
  - No resuelve CUIT por nombre: eso se hace en SQL contra el padrón (resolver_cuit).
  - Cada cambio de reglas debe subir PARSER_VERSION para poder re-parsear lo viejo.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from typing import Optional

PARSER_VERSION = "2.3.2"

# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
_RUIDO_LINEA = re.compile(
    r"^(Bolet[ií]n Oficial de la Ciudad|Gobierno de la Ciudad Aut[oó]noma|[“\"]?\s*\d{4}\s*[-.]|[“\"].*A[ñn]o d|…|\.{5,}|Inicia:)",
    re.I,
)

def limpiar_lineas(texto: str) -> list[str]:
    """Líneas con espacios colapsados, sin encabezados de página repetidos."""
    out = []
    for raw in (texto or "").replace("\r", "").split("\n"):
        l = re.sub(r"[ \t ]+", " ", raw).strip()
        if not l or _RUIDO_LINEA.match(l):
            continue
        out.append(l)
    return out


def a_numero(s: str) -> Optional[float]:
    """'5.426.800.-' -> 5426800 ; '4.228.180 ,00' -> 4228180.0 ; '2967,16' -> 2967.16"""
    if not s:
        return None
    s = re.sub(r"\s+", "", s).rstrip(".-")
    if not s or not s[0].isdigit():
        return None
    if "," in s:
        entero, _, dec = s.rpartition(",")
        entero = entero.replace(".", "")
        if not entero.isdigit() or not dec.isdigit():
            return None
        return float(f"{entero}.{dec}")
    if "." in s:
        partes = s.split(".")
        if all(len(p) == 3 for p in partes[1:]):
            return float("".join(partes))
        if len(partes) == 2 and len(partes[1]) in (1, 2):
            return float(s)
        # "8.646.000.00": miles con punto y centavos con punto
        if len(partes) > 2 and all(len(p) == 3 for p in partes[1:-1]) and len(partes[-1]) in (1, 2):
            return float("".join(partes[:-1]) + "." + partes[-1])
        return None
    return float(s)


_MONTO = re.compile(r"(?:\$|ARS|U\$S|USD)\s*(\d[\d.]*(?:\s*,\s*\d{1,2})?(?:\.-|-)?)")

def primer_monto(s: str) -> Optional[float]:
    r = primer_monto_moneda(s)
    return r[0] if r else None


MONTO_MAXIMO = 1e12   # ningún acto del BO supera el billón de pesos: más que eso es un error de tipeo/extracción


def monto_valido(v: Optional[float]) -> bool:
    return v is not None and 0 < v <= MONTO_MAXIMO


def primer_monto_moneda(s: str) -> Optional[tuple[float, str]]:
    for m in _MONTO.finditer(s):
        v = a_numero(m.group(1))
        if monto_valido(v):
            moneda = "USD" if re.match(r"(U\$S|USD)", m.group(0)) else "ARS"
            return v, moneda
    return None


def cuit_valido(c: str) -> bool:
    if not re.fullmatch(r"\d{11}", c):
        return False
    pesos = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2]
    s = sum(int(d) * p for d, p in zip(c[:10], pesos))
    dv = 11 - s % 11
    dv = 0 if dv == 11 else 9 if dv == 10 else dv
    return dv == int(c[10])


def clave_firma(nombre: str) -> str:
    """Clave SOLO para deduplicar dentro de un acto (la normalización real es SQL)."""
    k = re.sub(r"[^A-Z0-9]", "", nombre.upper()
               .translate(str.maketrans("ÁÉÍÓÚÜÑ", "AEIOUUN")))
    k = re.sub(r"SOCIEDADANONIMA$", "SA", k)
    return k


# ---------------------------------------------------------------------------
# Código de proceso, tipo y etapa
# ---------------------------------------------------------------------------
_COD_BAC = re.compile(r"\b(\d{2,5})\s?-\s?(\d{4})\s?-\s?([A-Z]{3})(\d{2})\b")
_COD_VIEJO = re.compile(r"N\s*[°º]\s*(\d{1,6})\s*/\s*(SIGAF)\s*/\s*(\d{2,4})\b")

TIPO_A_SIGLA = {
    "licitacion publica": "LPU", "licitacion obra publica": "LPU",
    "contratacion menor": "CME", "contratacion directa": "CDI",
    "licitacion privada": "LPR", "licitacion publica abreviada": "LPA",
}

ETAPAS = [
    (r"pre\s*-?\s*adjudicaci", "preadjudicacion"),
    (r"adjudicaci", "adjudicacion"),
    (r"llamado", "llamado"),
    (r"pr[oó]rroga", "prorroga"),
    (r"circular\s+con\s+consulta", "circular_con_consulta"),
    (r"circular\s+sin\s+consulta", "circular_sin_consulta"),
    (r"circular", "circular"),
    (r"apertura", "apertura"),
    (r"ampliaci", "ampliacion"),
    (r"(rectificaci|correcci|fe de erratas)", "rectificacion"),
    (r"(deja(r|se)? sin efecto|anulaci|revocaci)", "deja_sin_efecto"),
    (r"(fracasad|desiert)", "fracasada"),
    (r"(post\s*-?\s*ergaci|postergaci|modificaci)", "modificacion"),
]

def normalizar_etapa(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.lower()
    for patron, etapa in ETAPAS:
        if re.search(patron, s):
            return etapa
    return None


def _sin_acentos(s: str) -> str:
    return s.lower().translate(str.maketrans("áéíóúü", "aeiouu"))


def parsear_nombre_norma(nombre: str) -> tuple[Optional[str], Optional[str]]:
    """'Contratación Menor / Adjudicación  N° 116/HGACA/23' -> ('Contratación Menor', 'adjudicacion')"""
    if not nombre or "/" not in nombre:
        return None, None
    tipo, resto = [p.strip() for p in nombre.split("/", 1)]
    etapa_txt = re.split(r"\bN\s*[°º]", resto, maxsplit=1)[0]
    return tipo or None, normalizar_etapa(etapa_txt)


_TITULO = re.compile(r"^([A-Za-zÁÉÍÓÚÑáéíóúñ ]{4,40}?)\s+-\s+\S", re.I)

def etapa_desde_texto(lineas: list[str]) -> Optional[str]:
    """La línea título del acto: 'Preadjudicación - Contratación Menor Nº 434-0074-CME24'."""
    for l in lineas[:15]:
        m = _TITULO.match(l)
        if m:
            e = normalizar_etapa(m.group(1))
            if e:
                return e
    return None


# ---------------------------------------------------------------------------
# Monto total, ofertas
# ---------------------------------------------------------------------------
_ETIQUETAS_TOTAL = [
    r"total\s+(?:obra\s+)?(?:pre\s*-?\s*)?adjudicad[oa]s?",
    r"total\s+monto\s+(?:pre\s*-?\s*)?adjudicad[oa]",
    r"monto\s+total\s+(?:pre\s*-?\s*)?adjudicad[oa]",
    r"precio\s+total\s+de\s+la\s+pre\s*-?\s*adjudicaci[oó]n",
    r"total\s*,?\s+(?:de\s+la\s+)?(?:pre\s*-?\s*)?adjudicaci[oó]n",
    r"(?:en|por)\s+la\s+suma\s+total\s+de",
    r"cotiza\s+la\s+suma\s+total\s+de",
    r"monto\s+total",
    r"total\s+general",
    r"monto\s+adjudicado",
]

def monto_total(plano: str) -> tuple[Optional[float], Optional[str]]:
    for etiqueta in _ETIQUETAS_TOTAL:
        for m in re.finditer(etiqueta, plano, re.I):
            r = primer_monto_moneda(plano[m.end(): m.end() + 450])
            if r is not None:
                return r
    return None, None


_NUM_PAL = {"un": 1, "una": 1, "uno": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5,
            "seis": 6, "siete": 7, "ocho": 8, "nueve": 9, "diez": 10}

def cantidad_ofertas(plano: str) -> Optional[int]:
    pats = [
        r"ofertas?\s+presentadas?\s*:?\s*(\d+|[a-z]+)\b",
        r"cantidad\s+de\s+ofertas?\s*(?:presentadas?|recibidas?)?\s*:?\s*(\d+|[a-z]+)\b",
        r"se\s+(?:recibieron|presentaron)\s+(\d+|[a-z]+)\s*(?:\(\w+\)\s*)?ofertas?",
    ]
    for p in pats:
        m = re.search(p, plano, re.I)
        if m:
            g = m.group(1).lower()
            if g.isdigit():
                return int(g)
            if g in _NUM_PAL:
                return _NUM_PAL[g]
    return None


# ---------------------------------------------------------------------------
# Firmas
# ---------------------------------------------------------------------------
@dataclass
class Firma:
    nombre: str
    rol: str                      # ganadora | oferente | desestimada | mencionada
    monto: Optional[float] = None
    cuit_texto: Optional[int] = None


_SUFIJO = r"(S\.?\s?A\.?(?:\s?[ICFUS]\.?){0,6}|S\.?\s?R\.?\s?L\.?|S\.?\s?A\.?\s?S\.?|U\.?\s?T\.?\s?E\.?|SRL|SA|SAS|UTE|I\.?C\.?I\.?S\.?A\.?|Sociedad\s+An[oó]nima|LTDA\.?|S\.?\s?H\.?)"
_TERMINA_SUFIJO = re.compile(_SUFIJO + r"\s*\)?\s*$", re.I)

_RENGLON = re.compile(r"^(rengl[oó]n|renglones|r\s*\d|\d+(\.\d+)?\s+\S)", re.I)
_TABULAR = re.compile(r"^\d+(?:\.\d+)?\s+(.+?)\s+\d[\d.]*,\d{2}\s+(?:Unidad|U\b|Unid|Kg|Litro|Caja|Metro)", re.I)
_EN_RENGLON = re.compile(r"-\s*([^-]*?" + _SUFIJO + r")\s*-", re.I)

_STOP = re.compile(
    r"^(total|monto|precio total|renglones? desiert|fundament|encuadre|lugar|vencimiento|"
    r"observaci|consultas|dejar sin efecto|acto|domicilio|ofertas|inicia|clase|rubro|objeto|"
    r"expediente|aprobaci|comisi[oó]n|periodo|per[ií]odo|nota|importante|se deja|miembros|plazo|legal)",
    re.I,
)

_ETIQ_FIRMA = re.compile(
    r"^(?:(?:firma|firmas|firma\(s\)|firma/s)\s*(?:pre\s*-?\s*)?adjudicad[ao]\S*"
    r"|(?:firma\s+)?adjudicatari[ao]s?"
    r"|ofertas?\s+(?:pre\s*-?\s*)?adjudicadas?"
    r"|proveedor(?:es)?(?:\s+(?:pre\s*-?\s*)?adjudicad[oa]s?)?\s*:"     # con ':' obligatorio
    r"|nombre\s+de\s+la\s+empresa\s+(?:pre\s*-?\s*)?adjudicada"
    r"|raz[oó]n\s+social(?:\s+de\s+la\s+empresa)?\s*:"
    r"|adj[uú]dicase\s+la\s+contrataci[oó]n\s+a\s+la\s+firma)\s*:?\s*(.*)$",
    re.I,
)
_ETIQ_DESEST = re.compile(
    r"^(?:ofertas?\s+desestimadas?|ofertas?\s+rechazadas?|ofertas?\s+no\s+consideradas?|no\s+se\s+considera[n]?"
    r"|se\s+desestima\w*\s+(?:lo\s+ofertado\s+por|la\s+oferta\s+de|las\s+ofertas\s+de))(?:\s+\w+mente)?\s*:?\s*(.*)$", re.I)
_ETIQ_EMPRESA_EXPLICITA = re.compile(r"^nombre\s+de\s+la\s+empresa", re.I)

_FIRMA_DOS_PUNTOS = re.compile(r"^(?P<nombre>.{2,100}?" + _SUFIJO + r")(?:\s*:\s*|\s+(?=(?:para|por)\b))(?P<resto>.*)$", re.I)

_TITULOS_FIRMANTE = re.compile(r"^(director|gerent|subdirector|presidente|vicepresidente|subsecretari|secretari|jefe|jefa|coordinador|ministr|subgerent|titular|a/c|p/p)", re.I)


def es_firmante(lineas: list[str], k: int) -> bool:
    """La línea k es un título de firmante, o un nombre seguido de uno ('Lilia Borelli' / 'Directora Médica')."""
    if _TITULOS_FIRMANTE.match(lineas[k]):
        return True
    return k + 1 < len(lineas) and bool(_TITULOS_FIRMANTE.match(lineas[k + 1]))


def parece_firma(l: str) -> bool:
    l = l.strip(" -.,;")
    if not (3 <= len(l) <= 150) or ":" in l or l.endswith(":"):
        return False
    if not re.search(r"[A-Za-zÁÉÍÓÚÑ]{2}", l) or not (l[0].isupper() or l[0].isdigit()):
        return False
    if _RENGLON.match(l) or _STOP.match(l) or _TITULOS_FIRMANTE.match(l):
        return False
    if re.search(r"\bpesos\b|\$|https?:|www\.|\bDNI\b|\bD\.N\.I", l, re.I):
        return False
    if len(re.findall(r"\b(RENG\w*|CANT\w*|PRECIO|UNITARIO|TOTAL|IMPORTE|ITEM|DESCRIPCI\w+|UNIDAD)\b", l.upper())) >= 2:
        return False
    # palabras sueltas de la estructura del acto, no nombres
    if re.fullmatch(r"(empresas?|firmas?|oferentes?|proveedor(es)?|anexo\s*\w*|fracasad[oa]s?|desiert[oa]s?|"
                    r"dictamen.*|acta.*|observaciones|renglones?.*)", l, re.I):
        return False
    if re.search(r"\w/\w+[-/]\w", l):                       # restos de URL
        return False
    # "Alejandra Gurgo - Augusto Araoz - Patricia Gaeta": lista de evaluadores
    if l.count(" - ") >= 2 and not _TERMINA_SUFIJO.search(l):
        return False
    return len(l.split()) <= 14


_CORTE_NOMBRE = re.compile(
    r"^.*(?:\ba\s+favor\s+de(?:\s+la\s+firma)?|\ba\s+la\s+firma|\bla\s+empresa|\bpor\s+la\s+empresa|"
    r"\bfirma\s+adjudicataria|\bproveedor|\boferente)\s*[:\"“]?\s*", re.I)


def limpiar_nombre(n: str) -> str:
    n = re.sub(r"\s+", " ", n).strip(" -.,;:")
    n = _CORTE_NOMBRE.sub("", n)                         # "... a favor de la firma X" -> "X"
    n = re.sub(r"^[\d.,\s$-]+(?:-\s*)?", "", n)          # "14.848.000.- X" -> "X"
    n = n.strip(" -.,;:\"“”'")
    if n and n[0].islower():                             # arrastre de frase: "gún asesoramiento ... X"
        n = re.split(r"[.,;]\s+(?=[A-ZÁÉÍÓÚÑ])", n)[-1]
    n = re.sub(r"^(la\s+)?firma\s*:?\s*", "", n, flags=re.I)
    # "Por inadmisible. Tecnoimagen S.A." -> "Tecnoimagen S.A."
    n = re.sub(r"^.*\b[a-záéíóúñ]{3,}\.\s+(?=[A-ZÁÉÍÓÚÑ])", "", n)
    # restaurar el punto final de la sigla societaria si se lo comió el strip
    if re.search(r"\b(S\.A|S\.R\.L|S\.A\.S|S\.A\.I\.C|S\.A\.C\.I\.F|I\.C\.I\.S\.A)$", n, re.I):
        n += "."
    return n


_CUIT_EN_TEXTO = re.compile(
    r"([A-Za-zÁÉÍÓÚÑáéíóúñ0-9][^():;]{1,140}?)\s*\(\s*C\.?\s?U\.?\s?I\.?\s?T\.?\s*(?:N\s*[°ºo]\.?)?\s*:?\s*"
    r"(\d{2})\s*-?\s*(\d{8})\s*-?\s*(\d)\s*\)"
)

_CUIT_GUION = re.compile(r"(?:^|\s{2,}|^\d+(?:\.\d+)?\s+)([A-Za-zÁÉÍÓÚÑ][^:;()\n]{1,90}?)"
                         r"(?:\s+-\s+(?:C\.?\s?U\.?\s?I\.?\s?T\.?\s*(?:N\s*[°º])?\s*:?\s*)?"
                         r"|\s*,?\s+C\.?\s?U\.?\s?I\.?\s?T\.?\s*(?:N\s*[°º])?\s*:?\s*)(\d{2}-?\d{8}-?\d)\b")


def _rol_por_contexto(previo: str) -> Optional[str]:
    claves = [
        (r"desestimad|inadmisib|rechazad", "desestimada"),
        (r"adjudica|adjudicatari|proveedor|raz[oó]n social|a favor|recomienda", "ganadora"),
        (r"presentaci[oó]n efectuada|evaluaci[oó]n de la", "oferente"),
        (r"oferent|ofertas de las firmas", "oferente"),
    ]
    mejor, pos = None, -1
    for patron, rol in claves:
        for m in re.finditer(patron, previo, re.I):
            if m.start() > pos:
                mejor, pos = rol, m.start()
    return mejor


def _partir_lista(s: str) -> list[str]:
    """Separa 'A S.A., B S.A. y C S.A.' respetando paréntesis y 'Construcciones y Servicios'."""
    partes, buf, prof = [], "", 0
    for ch in s:
        if ch == "(":
            prof += 1
        elif ch == ")":
            prof -= 1
        if ch == "," and prof == 0:
            partes.append(buf); buf = ""
        else:
            buf += ch
    partes.append(buf)
    if partes:
        ult = partes[-1]
        # partir el último por ' y ' solo si lo anterior termina en sigla societaria o ')'
        cortes = [m.start() for m in re.finditer(r"\s+y\s+", ult)]
        for c in reversed(cortes):
            izq = ult[:c]
            if izq.count("(") == izq.count(")") and (_TERMINA_SUFIJO.search(izq) or izq.rstrip().endswith(")")):
                partes[-1:] = [izq, ult[c:].strip()[1:].strip()]
                break
    return [limpiar_nombre(p) for p in partes if limpiar_nombre(p)]


def extraer_firmas(lineas: list[str], plano: str) -> tuple[list[Firma], bool]:
    firmas: list[Firma] = []

    # (a) Firmas con CUIT explícito: el dato más confiable
    for m in _CUIT_EN_TEXTO.finditer(plano):
        nombre = limpiar_nombre(m.group(1))
        cuit = "".join(m.group(2, 3, 4))
        rol = _rol_por_contexto(plano[max(0, m.start() - 250): m.start() + len(m.group(1))]) or "mencionada"
        if nombre:
            firmas.append(Firma(nombre, rol, cuit_texto=int(cuit) if cuit_valido(cuit) else None))

    for l in lineas:
        for m in _CUIT_GUION.finditer(l):
            cuit = m.group(2).replace("-", "")
            nombre = limpiar_nombre(m.group(1))
            idx = plano.find(m.group(0)[:30])
            previo = plano[max(0, idx - 250): idx] if idx >= 0 else ""
            if nombre and cuit_valido(cuit):
                firmas.append(Firma(nombre, _rol_por_contexto(previo) or "mencionada", cuit_texto=int(cuit)))

    # (b) Bloques con etiqueta ("Firma(s) adjudicada(s):", "Proveedor:", ...)
    hay_empresa_explicita = any(_ETIQ_EMPRESA_EXPLICITA.match(l) for l in lineas)
    i = 0
    while i < len(lineas):
        m = _ETIQ_FIRMA.match(lineas[i])
        if not m:
            i += 1
            continue
        etiqueta = lineas[i]
        inline = m.group(1).strip()
        # "Firmas preadjudicadas: Nicolás Maure, ..." son evaluadores cuando hay "Nombre de la empresa"
        ignorar_inline = hay_empresa_explicita and not _ETIQ_EMPRESA_EXPLICITA.match(etiqueta)
        actual: Optional[Firma] = None
        if inline and not ignorar_inline and "(CUIT" not in inline.upper() and parece_firma(inline):
            actual = Firma(limpiar_nombre(inline), "ganadora")
            firmas.append(actual)
        j = i + 1
        renglones: dict[int, float] = {}       # id(firma) -> suma de totales de renglón
        renglon_roto: set[int] = set()
        en_bloque: list[Firma] = [actual] if actual else []
        while j < len(lineas):
            l = lineas[j]
            if _ETIQ_FIRMA.match(l) or _ETIQ_DESEST.match(l) or es_firmante(lineas, j):
                break
            if re.match(r"^(total|monto adjudicado|monto)\b", l, re.I):
                # total por firma ("Total: $ 503.640") o monto por firma ("Monto adjudicado: ...")
                es_total_general = re.match(r"^(total\s+(obra\s+|monto\s+)?(pre\s*-?\s*)?adjudica|monto\s+total|precio\s+total|total\s+general)", l, re.I)
                if es_total_general:
                    break
                v = primer_monto(" ".join(lineas[j:j + 3]))
                if actual is not None and v is not None and actual.monto is None:
                    propio = renglones.get(id(actual))
                    tot_bloque = sum((f.monto or renglones.get(id(f), 0.0)) for f in en_bloque)
                    # si coincide con la suma de todo el bloque y no con lo propio, es el total general
                    if (len(en_bloque) > 1 and abs(v - tot_bloque) <= 0.01 * v
                            and (propio is None or abs(v - propio) > 0.01 * v)):
                        break
                    actual.monto = v
                j += 1
                continue
            if _STOP.match(l):
                break
            if _RENGLON.match(l):
                mt = _TABULAR.match(l) or _EN_RENGLON.search(l)
                if mt:
                    firmas.append(Firma(limpiar_nombre(mt.group(1)), "ganadora"))
                elif actual is not None:
                    sig = lineas[j + 1] if j + 1 < len(lineas) else ""
                    texto_r = l + (" " + sig if re.search(r"(total|\$)\s*:?\s*\$?\s*$", l, re.I) else "")
                    montos = [a_numero(m.group(1)) for m in _MONTO.finditer(texto_r)]
                    montos = [m for m in montos if m]
                    if montos and not monto_valido(montos[-1]):
                        renglon_roto.add(id(actual))      # renglón ilegible: la suma de la firma no es confiable
                    elif montos:
                        renglones[id(actual)] = renglones.get(id(actual), 0.0) + montos[-1]
                j += 1
                continue
            if "(CUIT" in l.upper() or "(C.U.I.T" in l.upper():
                j += 1          # ya capturada en (a)
                continue
            mf = _FIRMA_DOS_PUNTOS.match(l)
            sig = lineas[j + 1] if j + 1 < len(lineas) else ""
            if mf:
                resto = mf.group("resto").strip()
                actual = Firma(limpiar_nombre(mf.group("nombre")), "ganadora",
                               monto=primer_monto(" ".join([resto] + lineas[j + 1:j + 3])) if resto else None)
                firmas.append(actual); en_bloque.append(actual)
            elif l.endswith(":") and parece_firma(l[:-1]) and _RENGLON.match(sig):
                actual = Firma(limpiar_nombre(l[:-1]), "ganadora")
                firmas.append(actual); en_bloque.append(actual)
            elif parece_firma(l):
                actual = Firma(limpiar_nombre(l), "ganadora")
                firmas.append(actual); en_bloque.append(actual)
            j += 1
        for f in en_bloque:
            if f.monto is None and renglones.get(id(f)) and id(f) not in renglon_roto:
                f.monto = round(renglones[id(f)], 2)
        i = j

    # (b2) Ofertas desestimadas / no consideradas (con o sin CUIT)
    for i, l in enumerate(lineas):
        m = _ETIQ_DESEST.match(l)
        if not m:
            continue
        # si la etiqueta trae el nombre en la misma línea, no se leen las siguientes
        candidatos = [m.group(1)] if m.group(1).strip() else [""] + lineas[i + 1:i + 12]
        for k, c in enumerate(candidatos):
            if k > 0 and (_STOP.match(c) or _ETIQ_FIRMA.match(c) or _ETIQ_DESEST.match(c)):
                break
            sig = candidatos[k + 1] if k + 1 < len(candidatos) else ""
            if _TITULOS_FIRMANTE.match(sig):      # "Hugo Pandullo" / "Director"
                break
            if "CUIT" in c.upper():
                continue
            nombre = re.sub(r"\s*[:(].*$", "", c).strip()
            if not (_TERMINA_SUFIJO.search(nombre) or len(nombre.split()) >= 2):
                continue
            if (nombre and parece_firma(nombre) and re.match(r"^[A-ZÁÉÍÓÚÑ]", nombre)
                    and not re.search(r"\b(del|de la|Pliego|Art[ií]culos?|Ley|Decreto|Renglon(es)?|Renglón)\b", nombre)):
                firmas.append(Firma(limpiar_nombre(nombre), "desestimada"))

    # (c) Línea con nombre seguida de un renglón, sin etiqueta ("Biotronik Arg" / "Renglón 1 ...")
    for k in range(len(lineas) - 1):
        if parece_firma(lineas[k]) and re.match(r"^(rengl[oó]n|r\s*\d)", lineas[k + 1], re.I) and not _ETIQ_FIRMA.match(lineas[k]):
            if k > 0 and _ETIQ_FIRMA.match(lineas[k - 1]):
                continue
            firmas.append(Firma(limpiar_nombre(lineas[k]), "ganadora"))

    # (d) Formato tabular ("1.1 Cirugia Argentina S.A. 6120,00 Unidad ARS ...")
    for l in lineas:
        mt = _TABULAR.match(l)
        if mt:
            firmas.append(Firma(limpiar_nombre(mt.group(1)), "ganadora"))

    # (e) "... a favor de: X"
    for m in re.finditer(r"favor\s+de\s*:?\s*(?:la\s+firma\s*:?\s*)?([^\n]+)", "\n".join(lineas), re.I):
        cand = re.split(r"\s+(?:Los|El|La|Las|por|en la suma|para|con)\b|\s+\(", m.group(1))[0]
        cand = limpiar_nombre(cand)
        if cand and parece_firma(cand):
            firmas.append(Firma(cand, "ganadora"))

    # (f) "fueron analizadas las ofertas de las firmas: A, B y C."
    m = re.search(
        r"(?:ofertas\s+de\s+las\s+(?:firmas|empresas)|presentaci[oó]n(?:es)?\s+efectuadas?\s+por\s+las\s+(?:firmas|empresas)"
        r"|correspondientes\s+a\s+las\s+(?:firmas|empresas)|se\s+presentaron\s+las\s+(?:firmas|empresas)"
        r"|oferentes\s*:)\s*:?\s*(.+?)"
        r"(?:;|\.\s+(?=[A-ZÁÉÍÓÚ][a-záéíóú]*\s+[a-záéíóú]{2,})|,\s+(?:ninguna|las\s+cuales|que|quienes|cuyas?)\b|$)",
        plano)
    hubo_lista = False
    if m:
        for n in _partir_lista(m.group(1)):
            if re.match(r"^[A-ZÁÉÍÓÚÑ0-9]", n) and ":" not in n and len(n.split()) <= 12:
                firmas.append(Firma(n, "oferente"))
                hubo_lista = True

    return _separar_combinadas(_deduplicar(firmas)), hubo_lista


def _separar_combinadas(firmas: list[Firma]) -> list[Firma]:
    """'Diego Hernán Juan y Quimica Erovne S.A.' (lista mal partida) -> se quita la firma ya conocida."""
    salida = list(firmas)
    for f in firmas:
        if f.rol != "oferente":
            continue
        kf = clave_firma(f.nombre)
        for g in firmas:
            kg = clave_firma(g.nombre)
            if g is not f and len(kg) >= 4 and kg in kf and kg != kf:
                resto = re.sub(re.escape(g.nombre), "", f.nombre, flags=re.I)
                resto = limpiar_nombre(re.sub(r"^\s*(y|,)\s+|\s+(y|,)\s*$", "", resto.strip()))
                salida.remove(f)
                if resto and parece_firma(resto):
                    salida.append(Firma(resto, "oferente"))
                break
    return _deduplicar(salida)


def _deduplicar(firmas: list[Firma]) -> list[Firma]:
    prioridad = {"ganadora": 0, "desestimada": 1, "oferente": 2, "mencionada": 3}
    por_clave: dict[str, Firma] = {}
    orden: list[str] = []
    for f in firmas:
        if not f.nombre or len(clave_firma(f.nombre)) < 3:
            continue
        k = clave_firma(f.nombre)
        if k not in por_clave:
            por_clave[k] = f
            orden.append(k)
            continue
        e = por_clave[k]
        if prioridad[f.rol] < prioridad[e.rol]:
            e.rol = f.rol
        e.cuit_texto = e.cuit_texto or f.cuit_texto
        e.monto = e.monto if e.monto is not None else f.monto
        if f.cuit_texto and not e.cuit_texto:
            e.nombre = f.nombre
    return [por_clave[k] for k in orden]


# ---------------------------------------------------------------------------
# Resultado
# ---------------------------------------------------------------------------
@dataclass
class Acto:
    codigo_proceso: Optional[str] = None
    uoa: Optional[int] = None
    tipo_proc: Optional[str] = None
    anio_proc: Optional[int] = None
    tipo_licitacion: Optional[str] = None
    etapa: Optional[str] = None
    etapa_conflicto: bool = False
    expediente: Optional[str] = None
    objeto: Optional[str] = None
    monto_total: Optional[float] = None
    monto_origen: Optional[str] = None    # 'texto' | 'suma_firmas'
    moneda: Optional[str] = None
    n_ofertas: Optional[int] = None
    unica_oferta: Optional[bool] = None
    fracasada: bool = False
    n_firmas_identificadas: int = 0
    firmas: list[Firma] = field(default_factory=list)
    confianza: str = "baja"
    faltantes: list[str] = field(default_factory=list)
    parser_version: str = PARSER_VERSION

    def como_dict(self) -> dict:
        return asdict(self)


def rol_final(rol: str, etapa: Optional[str]) -> str:
    if rol == "ganadora":
        return "preadjudicada" if etapa == "preadjudicacion" else "adjudicada" if etapa == "adjudicacion" else "mencionada"
    return rol


def parsear_acto(nombre: Optional[str], texto: Optional[str]) -> Acto:
    a = Acto()
    lineas = limpiar_lineas(texto or "")
    plano = " ".join(lineas)

    # tipo y etapa
    a.tipo_licitacion, etapa_nombre = parsear_nombre_norma(nombre or "")
    etapa_texto = etapa_desde_texto(lineas)
    a.etapa = etapa_texto or etapa_nombre
    a.etapa_conflicto = bool(etapa_texto and etapa_nombre and etapa_texto != etapa_nombre)

    # código de proceso
    m = _COD_BAC.search(plano)
    if m:
        a.codigo_proceso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}{m.group(4)}"
        a.uoa, a.tipo_proc, a.anio_proc = int(m.group(1)), m.group(3), 2000 + int(m.group(4))
    else:
        m = _COD_VIEJO.search(plano)
        if m:
            anio = int(m.group(3))
            a.anio_proc = anio + 2000 if anio < 100 else anio
            a.codigo_proceso = f"{int(m.group(1))}/{m.group(2)}/{str(anio)[-2:]}"
    if a.tipo_proc is None and a.tipo_licitacion:
        a.tipo_proc = TIPO_A_SIGLA.get(_sin_acentos(a.tipo_licitacion))

    # expediente y objeto
    for l in lineas[:20]:
        if re.match(r"^(EX[-\s]|E\.?\s?E\.?|EE\b|Expediente)", l, re.I):
            a.expediente = l[:120]
            break
    mo = re.search(r"\bObjeto(?:\s+de\s+la\s+contrataci[oó]n)?\s*:?\s*(.{5,300}?)(?=\s+(?:Acto|Autorizante|Actua|Firma|Fecha|Ofertas|Clase|Rubro|Repartici|Norma|Domicilio|Total|Renglón)\b|$)", plano, re.I)
    if mo:
        a.objeto = mo.group(1).strip(' ."“”')[:300]

    # montos y ofertas
    a.monto_total, a.moneda = monto_total(plano)
    a.monto_origen = "texto" if a.monto_total is not None else None
    if a.monto_total is None:
        # respaldo: la última línea que empieza con "Total" y trae un monto
        for k in range(len(lineas) - 1, -1, -1):
            l = lineas[k]
            previa = lineas[k - 1] if k else ""
            # "Precio \nTotal: $ x" es el total de un renglón partido en dos líneas, no el del acto
            if re.match(r"^total\b", l, re.I) and not re.search(r"precio\s*$", previa, re.I):
                r = primer_monto_moneda(l)
                if r:
                    a.monto_total, a.moneda = r
                    a.monto_origen = "texto"
                    break
    a.n_ofertas = cantidad_ofertas(plano)

    firmas, hubo_lista = extraer_firmas(lineas, plano)
    for f in firmas:
        f.rol = rol_final(f.rol, a.etapa)
    a.firmas = firmas

    ganadoras = [f for f in firmas if f.rol in ("adjudicada", "preadjudicada")]
    if (a.monto_total is None and ganadoras and all(f.monto for f in ganadoras)
            and monto_valido(sum(f.monto for f in ganadoras))):
        a.monto_total, a.moneda = round(sum(f.monto for f in ganadoras), 2), "ARS"
        a.monto_origen = "suma_firmas"

    # n_firmas_identificadas: cota inferior de oferentes (todas las firmas con rol de oferta).
    # n_ofertas: solo si el texto lo dice o trae la lista explícita de oferentes.
    con_oferta = [f for f in firmas if f.rol in ("oferente", "desestimada", "adjudicada", "preadjudicada")]
    a.n_firmas_identificadas = len(con_oferta)
    if a.n_ofertas is None and (hubo_lista or any(f.rol == "oferente" for f in firmas)):
        a.n_ofertas = len(con_oferta)
    if re.search(r"[uú]nica\s+oferta|oferta\s+[uú]nica|[uú]nico\s+oferente", plano, re.I):
        a.unica_oferta = True
    elif a.n_ofertas is not None:
        a.unica_oferta = a.n_ofertas == 1

    a.fracasada = bool(re.search(
        r"licitaci[oó]n\s+fracasada|(?:se\s+)?declar\w*\s+(?:fracasad|desiert)|fracasad[oa]\s+por|sin\s+efecto\s+por\s+inexistencia",
        plano, re.I))

    # confianza
    if a.etapa in ("adjudicacion", "preadjudicacion") and not a.fracasada:
        req = {"codigo_proceso": a.codigo_proceso, "monto_total": a.monto_total,
               "firma": any(f.rol in ("adjudicada", "preadjudicada") for f in firmas)}
    else:
        req = {"codigo_proceso": a.codigo_proceso, "etapa": a.etapa}
    a.faltantes = [k for k, v in req.items() if not v]
    n_ok = len(req) - len(a.faltantes)
    a.confianza = "alta" if n_ok == len(req) else "media" if n_ok >= len(req) - 1 else "baja"
    return a
