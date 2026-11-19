"""Línea de comandos: python -m monitor_licitaciones <comando> ...

Comandos (todos idempotentes; se pueden re-ejecutar sin duplicar):
  init-db                       crea/actualiza esquemas, tablas, funciones y vistas
  migrar-v1                     copia normas, textos y padrón de empresas desde public (v1)
  boletines --desde --hasta     baja boletines y normas de la API
  pdfs [--limite N]             baja PDFs pendientes y extrae texto
  parsear [--todo]              parsea actos de licitación (solo nuevos o de versión vieja)
  resolver                      resuelve CUIT de las firmas (texto, alias, padrón)
  bac [--archivo f.csv]         guarda un snapshot del OCDS de BAC
  ipc --csv f.csv               carga serie IPC (columnas: mes=AAAA-MM, indice)
  actualizar                    corrida diaria: boletines desde la última fecha + pdfs + parsear + resolver
  calidad                       muestra cobertura y calidad de la extracción
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from . import bac as bac_mod
from .api_bo import BoletinAPI
from .db import conectar, ejecucion, init_db
from .ingesta import cargar_boletines, parsear_documentos, procesar_pdfs

log = logging.getLogger("monitor_licitaciones")


def _fecha(s: str) -> date:
    return date.fromisoformat(s)


def _imprimir(cur) -> None:
    filas = cur.fetchall()
    cols = [d.name for d in cur.description]
    anchos = [max(len(str(c)), *(len(str(f[i])) for f in filas)) if filas else len(c) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(a) for c, a in zip(cols, anchos)))
    for f in filas:
        print("  ".join(str(v).ljust(a) for v, a in zip(f, anchos)))
    print()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="monitor-licitaciones", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # --dsn y -v valen antes o después del comando (p.ej. "parsear -v" o "-v parsear")
    comunes = argparse.ArgumentParser(add_help=False)
    comunes.add_argument("--dsn", default=argparse.SUPPRESS, help="cadena de conexión (por defecto MLC_DSN)")
    comunes.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS)
    p.add_argument("--dsn", help="cadena de conexión (por defecto MLC_DSN)")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)
    _add = sub.add_parser
    sub.add_parser = lambda *x, **k: _add(*x, parents=[comunes], **k)
    sub.add_parser("init-db")
    sub.add_parser("migrar-v1")
    b = sub.add_parser("boletines")
    b.add_argument("--desde", type=_fecha, required=True)
    b.add_argument("--hasta", type=_fecha, default=date.today())
    b.add_argument("--forzar", action="store_true", help="re-consultar fechas ya consultadas")
    b.add_argument("--pausa", type=float, default=0.5, help="segundos entre pedidos a la API")
    d = sub.add_parser("pdfs")
    d.add_argument("--limite", type=int, default=500)
    d.add_argument("--max-intentos", type=int, default=3)
    pa = sub.add_parser("parsear")
    pa.add_argument("--todo", action="store_true", help="re-parsear todo, aunque la versión coincida")
    sub.add_parser("resolver")
    bc = sub.add_parser("bac")
    bc.add_argument("--archivo", type=Path, help="CSV ya descargado (si no, se baja de datos abiertos)")
    ip = sub.add_parser("ipc")
    ip.add_argument("--csv", type=Path, required=True)
    ac = sub.add_parser("actualizar")
    ac.add_argument("--limite-pdfs", type=int, default=2000)
    sub.add_parser("calidad")
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO if a.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(message)s")
    conn = conectar(a.dsn)

    if a.cmd == "init-db":
        for f in init_db(conn):
            print("aplicado", f)
        return 0

    with ejecucion(conn, a.cmd, {k: v for k, v in vars(a).items() if k not in ("dsn", "cmd")}) as ej:
        if a.cmd == "migrar-v1":
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM ctl.migrar_v1()")
                _imprimir(cur)
            conn.commit()

        elif a.cmd == "boletines":
            cargar_boletines(conn, ej, BoletinAPI(pausa=a.pausa), a.desde, a.hasta, a.forzar)

        elif a.cmd == "pdfs":
            procesar_pdfs(conn, ej, limite=a.limite, max_intentos=a.max_intentos)

        elif a.cmd == "parsear":
            parsear_documentos(conn, ej, todo=a.todo)

        elif a.cmd == "resolver":
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM ref.resolver_cuit()")
                _imprimir(cur)
            conn.commit()

        elif a.cmd == "bac":
            if a.archivo:
                path, lm = a.archivo, None
            else:
                path, lm = bac_mod.descargar()
            bac_mod.cargar_snapshot(conn, ej, path, last_modified=lm)

        elif a.cmd == "ipc":
            with a.csv.open(encoding="utf-8-sig") as f, conn.cursor() as cur:
                for row in csv.DictReader(f):
                    mes = date.fromisoformat(row["mes"][:7] + "-01")
                    cur.execute("""INSERT INTO ref.ipc (mes, indice) VALUES (%s, %s)
                                   ON CONFLICT (mes) DO UPDATE SET indice = EXCLUDED.indice""",
                                (mes, float(row["indice"].replace(",", "."))))
                    ej.sumar("meses")
            conn.commit()

        elif a.cmd == "actualizar":
            with conn.cursor() as cur:
                cur.execute("SELECT coalesce(max(fecha), date '2019-12-01') FROM ctl.fecha_consultada "
                            "WHERE resultado = 'con_boletin'")
                desde = cur.fetchone()[0] + timedelta(days=1)
            cargar_boletines(conn, ej, BoletinAPI(), desde, date.today())
            procesar_pdfs(conn, ej, limite=a.limite_pdfs)
            parsear_documentos(conn, ej)
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM ref.resolver_cuit()")
            conn.commit()

        elif a.cmd == "calidad":
            with conn.cursor() as cur:
                for titulo, q in [
                    ("Documentos (texto de PDFs)", "SELECT estado, sum(documentos) documentos, sum(agotaron_reintentos) agotados FROM analitica.calidad_documentos GROUP BY 1 ORDER BY 2 DESC"),
                    ("Parser por etapa (todas las fechas)", "SELECT etapa, sum(actos) actos, round(sum(actos*pct_alta)/sum(actos),1) pct_alta, round(sum(actos*pct_con_monto)/sum(actos),1) pct_con_monto, round(sum(actos*pct_con_codigo)/sum(actos),1) pct_con_codigo FROM analitica.calidad_parser GROUP BY 1 ORDER BY 2 DESC"),
                    ("Resolución de CUIT por rol", "SELECT * FROM analitica.calidad_resolucion ORDER BY rol, firmas DESC"),
                    ("Errores abiertos", "SELECT etapa, count(*) FROM ctl.error WHERE resuelto_en IS NULL GROUP BY 1"),
                ]:
                    print(f"== {titulo}")
                    cur.execute(q)
                    _imprimir(cur)

    print({k: v for k, v in ej.resumen.items()} or "ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
