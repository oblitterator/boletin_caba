"""Snapshots del OCDS aplanado de Buenos Aires Compras (datos abiertos).

El CSV oficial (bac_anual.csv) es una ventana móvil: lo que no se guarde hoy se pierde.
Cada descarga se registra en bac.snapshot; las filas se upsertean por ocid conservando
primer y último snapshot en que se vieron. Limitación conocida: el CSV no trae oferentes
(tenderers), solo adjudicatarios.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Iterator, Optional

import psycopg
import requests

from .db import Ejecucion

URL_BAC = ("https://cdn.buenosaires.gob.ar/datosabiertos/datasets/ministerio-de-economia-y-finanzas/"
           "buenos-aires-compras/bac_anual.csv")

_COD = re.compile(r"ocds-[a-z0-9]+-(\d{2,5}-\d{4}-[A-Z]{3}\d{2})")
_CUIT = re.compile(r"CUIT-(\d{2})-?(\d{8})-?(\d)")
_IDX_AWARD = re.compile(r"^awards/(\d+)/(.+)$")


def descargar(url: str = URL_BAC, timeout: float = 300) -> tuple[Path, Optional[str]]:
    r = requests.get(url, stream=True, timeout=(15, timeout), headers={"User-Agent": "monitor-licitaciones-caba/2"})
    r.raise_for_status()
    tmp = Path(tempfile.mkstemp(suffix=".csv")[1])
    with tmp.open("wb") as f:
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
    return tmp, r.headers.get("Last-Modified")


def _num(v: Optional[str]) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def _cuit(party_id: Optional[str]) -> Optional[int]:
    m = _CUIT.search(party_id or "")
    return int("".join(m.groups())) if m else None


def filas_csv(path: Path) -> Iterator[dict]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            yield {k: v for k, v in row.items() if k and v not in (None, "")}


def adjudicaciones_de_fila(row: dict) -> list[dict]:
    por_award: dict[str, dict] = defaultdict(dict)
    for k, v in row.items():
        m = _IDX_AWARD.match(k)
        if m:
            por_award[m.group(1)][m.group(2)] = v
    out = []
    for i, a in sorted(por_award.items()):
        sup_idx = sorted({re.match(r"suppliers/(\d+)/", k).group(1) for k in a if k.startswith("suppliers/")})
        for j in sup_idx or [None]:
            sid = a.get(f"suppliers/{j}/id") if j is not None else None
            out.append({
                "award_id": f"{a.get('id', i)}#{j if j is not None else 0}",
                "cuit": _cuit(sid),
                "proveedor": a.get(f"suppliers/{j}/name") if j is not None else None,
                "monto": _num(a.get("value/amount")),
                "moneda": a.get("value/currency"),
                "fecha": a.get("date"),
                "estado": a.get("status"),
            })
    return out


def cargar_snapshot(conn: psycopg.Connection, ej: Ejecucion, path: Path, url: str = URL_BAC,
                    last_modified: Optional[str] = None) -> Optional[int]:
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM bac.snapshot WHERE sha256 = %s", (sha,))
        if cur.fetchone():
            ej.resumen["snapshot"] = "sin cambios (mismo sha256)"
            return None
        cur.execute("INSERT INTO bac.snapshot (url, last_modified, sha256) VALUES (%s,%s,%s) RETURNING id",
                    (url, last_modified, sha))
        snap = cur.fetchone()[0]
        n = 0
        for row in filas_csv(path):
            ocid = row.get("ocid")
            if not ocid:
                continue
            m = _COD.search(ocid)
            cur.execute(
                """INSERT INTO bac.release (ocid, codigo_proceso, metodo, metodo_detalle, estado, comprador_id,
                       comprador, titulo, fecha, monto_estimado, primer_snapshot, ultimo_snapshot, raw)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (ocid) DO UPDATE SET
                     codigo_proceso=EXCLUDED.codigo_proceso, metodo=EXCLUDED.metodo,
                     metodo_detalle=EXCLUDED.metodo_detalle, estado=EXCLUDED.estado,
                     comprador_id=EXCLUDED.comprador_id, comprador=EXCLUDED.comprador, titulo=EXCLUDED.titulo,
                     fecha=EXCLUDED.fecha, monto_estimado=EXCLUDED.monto_estimado,
                     ultimo_snapshot=EXCLUDED.ultimo_snapshot, raw=EXCLUDED.raw""",
                (ocid, m.group(1) if m else None, row.get("tender/procurementMethod"),
                 row.get("tender/procurementMethodDetails"), row.get("tender/status"),
                 row.get("tender/procuringEntity/id"), row.get("tender/procuringEntity/name"),
                 row.get("tender/title"), row.get("date"), _num(row.get("tender/value/amount")), snap, snap,
                 json.dumps(row, ensure_ascii=False)),
            )
            for a in adjudicaciones_de_fila(row):
                cur.execute(
                    """INSERT INTO bac.adjudicacion (ocid, award_id, cuit, proveedor, monto, moneda, fecha, estado)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (ocid, award_id) DO UPDATE SET cuit=EXCLUDED.cuit, proveedor=EXCLUDED.proveedor,
                         monto=EXCLUDED.monto, moneda=EXCLUDED.moneda, fecha=EXCLUDED.fecha, estado=EXCLUDED.estado""",
                    (ocid, a["award_id"], a["cuit"], a["proveedor"], a["monto"], a["moneda"], a["fecha"], a["estado"]),
                )
                ej.sumar("adjudicaciones")
            n += 1
        cur.execute("UPDATE bac.snapshot SET filas = %s WHERE id = %s", (n, snap))
    conn.commit()
    ej.sumar("releases", n)
    ej.resumen["snapshot"] = snap
    return snap
