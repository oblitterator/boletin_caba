"""Carga del Boletín Oficial: boletines+normas, PDFs y parseo de actos. Todo idempotente."""
from __future__ import annotations

import hashlib
import io
import json
import logging
from datetime import date, timedelta
from typing import Callable, Iterable, Optional

import psycopg
import requests

from .api_bo import BoletinAPI, ErrorAPI, RespuestaBoletin
from .db import Ejecucion
from .parser_actos import PARSER_VERSION, parsear_acto

log = logging.getLogger("monitor_licitaciones")

# Secciones cuyas normas se bajan en PDF para extraer texto
SECCIONES_CON_PDF = ("Licitaciones",)


# ---------------------------------------------------------------------------
# Boletines y normas
# ---------------------------------------------------------------------------
def fechas_a_consultar(conn: psycopg.Connection, desde: date, hasta: date, forzar: bool) -> list[date]:
    todas = [desde + timedelta(days=i) for i in range((hasta - desde).days + 1)]
    if forzar:
        return todas
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fecha FROM ctl.fecha_consultada WHERE fecha BETWEEN %s AND %s AND resultado <> 'error'",
            (desde, hasta),
        )
        hechas = {r[0] for r in cur.fetchall()}
    return [f for f in todas if f not in hechas]


def _id_norma(n: dict) -> Optional[int]:
    for k in ("id_norma", "idNorma", "id"):
        v = n.get(k)
        if v not in (None, ""):
            try:
                return int(v)
            except (TypeError, ValueError):
                return None
    return None


def guardar_respuesta(conn: psycopg.Connection, r: RespuestaBoletin) -> dict:
    """Upsert de boletín y normas. Devuelve contadores."""
    c = {"normas": 0, "pdf_pendientes": 0}
    b = r.boletin
    fecha_pub = r.fecha_publicacion or r.fecha_consultada
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO bo.boletin (numero, fecha_publicacion, es_separata, nombre, url_boletin, raw)
               VALUES (%(numero)s, %(fecha)s, %(sep)s, %(nombre)s, %(url)s, %(raw)s)
               ON CONFLICT (numero) DO UPDATE SET
                 fecha_publicacion = EXCLUDED.fecha_publicacion, es_separata = EXCLUDED.es_separata,
                 nombre = EXCLUDED.nombre, url_boletin = EXCLUDED.url_boletin, raw = EXCLUDED.raw,
                 actualizado_en = now()
               WHERE bo.boletin.raw IS DISTINCT FROM EXCLUDED.raw""",
            {"numero": int(b["numero"]), "fecha": fecha_pub,
             "sep": str(b.get("separata", "")).lower() in ("1", "true", "si", "sí"),
             "nombre": b.get("nombre"), "url": b.get("url_boletin"),
             "raw": json.dumps(b, ensure_ascii=False, default=str)},
        )
        for n in r.normas:
            idn = _id_norma(n)
            if idn is None:
                continue
            raw = {k: v for k, v in n.items() if not k.startswith("_")}
            cur.execute(
                """INSERT INTO bo.norma (id_norma, boletin_numero, fecha_publicacion, seccion, tipo, organismo,
                                         nombre, sumario, url_norma, raw, origen)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'api')
                   ON CONFLICT (id_norma) DO UPDATE SET
                     boletin_numero = EXCLUDED.boletin_numero, fecha_publicacion = EXCLUDED.fecha_publicacion,
                     seccion = EXCLUDED.seccion, tipo = EXCLUDED.tipo, organismo = EXCLUDED.organismo,
                     nombre = EXCLUDED.nombre, sumario = EXCLUDED.sumario, url_norma = EXCLUDED.url_norma,
                     raw = EXCLUDED.raw, origen = 'api', actualizado_en = now()
                   WHERE bo.norma.raw IS DISTINCT FROM EXCLUDED.raw OR bo.norma.origen <> 'api'""",
                (idn, int(b["numero"]), fecha_pub, n.get("_seccion"), n.get("_tipo"), n.get("_organismo"),
                 n.get("nombre"), n.get("sumario"), n.get("url_norma") or n.get("url"),
                 json.dumps(raw, ensure_ascii=False, default=str)),
            )
            c["normas"] += 1
            if n.get("_seccion") in SECCIONES_CON_PDF:
                cur.execute(
                    "INSERT INTO bo.documento (id_norma, estado) VALUES (%s, 'pendiente') ON CONFLICT DO NOTHING",
                    (idn,),
                )
                c["pdf_pendientes"] += cur.rowcount
    return c


def marcar_fecha(conn: psycopg.Connection, fecha: date, resultado: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO ctl.fecha_consultada (fecha, resultado) VALUES (%s, %s)
               ON CONFLICT (fecha) DO UPDATE SET resultado = EXCLUDED.resultado, consultado_en = now()""",
            (fecha, resultado),
        )


def cargar_boletines(conn: psycopg.Connection, ej: Ejecucion, api: BoletinAPI,
                     desde: date, hasta: date, forzar: bool = False) -> None:
    for fecha in fechas_a_consultar(conn, desde, hasta, forzar):
        try:
            r = api.obtener_boletin(fecha)
        except ErrorAPI as e:
            ej.error("api_boletin", fecha.isoformat(), str(e))
            marcar_fecha(conn, fecha, "error")
            conn.commit()
            ej.sumar("fechas_error")
            log.warning("%s: %s", fecha, e)
            continue
        if r.boletin is None or (r.fecha_publicacion and r.fecha_publicacion != fecha):
            # sin edición ese día (la API puede devolver el boletín más cercano: se guarda igual)
            if r.boletin is not None and not r.error_normas:
                guardar_respuesta(conn, r)
            marcar_fecha(conn, fecha, "sin_boletin")
            conn.commit()
            ej.sumar("fechas_sin_boletin")
            continue
        if r.error_normas:
            ej.error("api_boletin", fecha.isoformat(), f"normas: {r.error_normas}")
            marcar_fecha(conn, fecha, "error")
            conn.commit()
            ej.sumar("fechas_error")
            continue
        c = guardar_respuesta(conn, r)
        marcar_fecha(conn, fecha, "con_boletin")
        ej.resolver("api_boletin", fecha.isoformat())
        conn.commit()
        ej.sumar("boletines")
        ej.sumar("normas", c["normas"])
        ej.sumar("pdf_pendientes", c["pdf_pendientes"])
        log.info("%s: boletín %s, %d normas", fecha, r.boletin.get("numero"), c["normas"])


# ---------------------------------------------------------------------------
# PDFs -> texto
# ---------------------------------------------------------------------------
def extraer_texto_pdf(contenido: bytes) -> tuple[str, int]:
    import pdfplumber  # import diferido: pesado
    with pdfplumber.open(io.BytesIO(contenido)) as pdf:
        paginas = [p.extract_text() or "" for p in pdf.pages]
    return "\n".join(paginas), len(paginas)


def procesar_pdfs(conn: psycopg.Connection, ej: Ejecucion, limite: int = 500, max_intentos: int = 3,
                  session: Optional[requests.Session] = None, timeout: float = 60,
                  descargar: Optional[Callable[[str], bytes]] = None) -> None:
    s = session or requests.Session()

    def _bajar(url: str) -> bytes:
        r = s.get(url, timeout=(10, timeout), headers={"User-Agent": "monitor-licitaciones-caba/2"})
        r.raise_for_status()
        return r.content

    descargar = descargar or _bajar
    with conn.cursor() as cur:
        cur.execute(
            """SELECT d.id_norma, n.url_norma FROM bo.documento d JOIN bo.norma n USING (id_norma)
               WHERE d.estado IN ('pendiente','error') AND d.intentos < %s AND n.url_norma IS NOT NULL
               ORDER BY n.fecha_publicacion DESC LIMIT %s""",
            (max_intentos, limite),
        )
        pendientes = cur.fetchall()
    for id_norma, url in pendientes:
        try:
            contenido = descargar(url)
            texto, paginas = extraer_texto_pdf(contenido)
            estado = "ok" if texto.strip() else "vacio"   # vacío = probablemente escaneado
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE bo.documento SET estado=%s, intentos=intentos+1, ultimo_error=NULL, sha256=%s,
                       paginas=%s, texto=%s, extraido_en=now(), actualizado_en=now() WHERE id_norma=%s""",
                    (estado, hashlib.sha256(contenido).hexdigest(), paginas, texto, id_norma),
                )
            ej.resolver("pdf", id_norma)
            ej.sumar(f"pdf_{estado}")
        except Exception as e:  # noqa: BLE001 - se registra y se sigue
            msg = f"{type(e).__name__}: {e}"
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE bo.documento SET estado='error', intentos=intentos+1, ultimo_error=%s,
                       actualizado_en=now() WHERE id_norma=%s""",
                    (msg[:1000], id_norma),
                )
            ej.error("pdf", id_norma, msg)
            ej.sumar("pdf_error")
        conn.commit()


# ---------------------------------------------------------------------------
# Parseo de actos
# ---------------------------------------------------------------------------
def guardar_acto(cur: psycopg.Cursor, id_norma: int, nombre: Optional[str], texto: str) -> str:
    a = parsear_acto(nombre, texto)
    cur.execute(
        """INSERT INTO bo.acto (id_norma, codigo_proceso, uoa, tipo_proc, anio_proc, tipo_licitacion, etapa,
              etapa_conflicto, expediente, objeto, monto_total, monto_origen, moneda, n_ofertas, n_firmas_identificadas,
              unica_oferta, fracasada, confianza, faltantes, parser_version, parseado_en)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
           ON CONFLICT (id_norma) DO UPDATE SET
              codigo_proceso=EXCLUDED.codigo_proceso, uoa=EXCLUDED.uoa, tipo_proc=EXCLUDED.tipo_proc,
              anio_proc=EXCLUDED.anio_proc, tipo_licitacion=EXCLUDED.tipo_licitacion, etapa=EXCLUDED.etapa,
              etapa_conflicto=EXCLUDED.etapa_conflicto, expediente=EXCLUDED.expediente, objeto=EXCLUDED.objeto,
              monto_total=EXCLUDED.monto_total, monto_origen=EXCLUDED.monto_origen, moneda=EXCLUDED.moneda, n_ofertas=EXCLUDED.n_ofertas,
              n_firmas_identificadas=EXCLUDED.n_firmas_identificadas, unica_oferta=EXCLUDED.unica_oferta,
              fracasada=EXCLUDED.fracasada, confianza=EXCLUDED.confianza, faltantes=EXCLUDED.faltantes,
              parser_version=EXCLUDED.parser_version, parseado_en=now()""",
        (id_norma, a.codigo_proceso, a.uoa, a.tipo_proc, a.anio_proc, a.tipo_licitacion, a.etapa,
         a.etapa_conflicto, a.expediente, a.objeto, a.monto_total, a.monto_origen, a.moneda, a.n_ofertas,
         a.n_firmas_identificadas, a.unica_oferta, a.fracasada, a.confianza, a.faltantes, a.parser_version),
    )
    cur.execute("DELETE FROM bo.acto_firma WHERE id_norma = %s", (id_norma,))
    for i, f in enumerate(a.firmas, 1):
        cur.execute(
            """INSERT INTO bo.acto_firma (id_norma, orden, nombre_raw, rol, monto, cuit_texto)
               VALUES (%s,%s,%s,%s,%s,%s)""",
            (id_norma, i, f.nombre[:300], f.rol, f.monto, f.cuit_texto),
        )
    return a.confianza


def parsear_documentos(conn: psycopg.Connection, ej: Ejecucion, todo: bool = False, lote: int = 1000) -> None:
    """Parsea los documentos OK sin acto, o con versión de parser vieja (o todos si todo=True).
    Un error en un documento se registra en ctl.error y no frena el resto."""
    filtro = "" if todo else "AND (a.id_norma IS NULL OR a.parser_version <> %(v)s)"
    ultimo = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT d.id_norma, n.nombre, d.texto FROM bo.documento d
                    JOIN bo.norma n USING (id_norma) LEFT JOIN bo.acto a USING (id_norma)
                    WHERE d.estado = 'ok' AND d.id_norma > %(ultimo)s {filtro}
                    ORDER BY d.id_norma LIMIT %(lote)s""",
                {"v": PARSER_VERSION, "lote": lote, "ultimo": ultimo},
            )
            filas = cur.fetchall()
        if not filas:
            break
        for id_norma, nombre, texto in filas:
            try:
                with conn.transaction():          # savepoint por documento
                    with conn.cursor() as cur:
                        conf = guardar_acto(cur, id_norma, nombre, texto or "")
                ej.sumar(f"confianza_{conf}")
            except Exception as e:  # noqa: BLE001
                ej.error("parser", id_norma, f"{type(e).__name__}: {e}")
                ej.sumar("parser_error")
        ultimo = filas[-1][0]
        conn.commit()
        ej.sumar("parseados", len(filas))
        log.info("parseados %d", ej.resumen["parseados"])
