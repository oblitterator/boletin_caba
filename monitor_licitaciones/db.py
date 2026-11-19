"""Conexión, ejecución de SQL y registro de ejecuciones/errores en ctl.*"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

import psycopg

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
DSN_DEFECTO = "postgresql://postgres@127.0.0.1:5432/boletin_oficial_caba"


def dsn() -> str:
    return os.environ.get("MLC_DSN", DSN_DEFECTO)


def conectar(dsn_: Optional[str] = None) -> psycopg.Connection:
    conn = psycopg.connect(dsn_ or dsn(), autocommit=False)
    with conn.cursor() as cur:
        cur.execute("SET application_name = 'monitor_licitaciones'")
    conn.commit()
    return conn


def ejecutar_archivo(conn: psycopg.Connection, nombre: str) -> None:
    sql = (SQL_DIR / nombre).read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def init_db(conn: psycopg.Connection) -> list[str]:
    """Aplica todos los .sql en orden. Todos son idempotentes."""
    aplicados = []
    for f in sorted(SQL_DIR.glob("*.sql")):
        ejecutar_archivo(conn, f.name)
        aplicados.append(f.name)
    return aplicados


class Ejecucion:
    """Registra una corrida en ctl.ejecucion y centraliza los errores en ctl.error."""

    def __init__(self, conn: psycopg.Connection, comando: str, parametros: dict | None = None):
        self.conn = conn
        self.resumen: dict[str, Any] = {}
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ctl.ejecucion (comando, parametros) VALUES (%s, %s) RETURNING id",
                (comando, json.dumps(parametros or {}, default=str)),
            )
            self.id = cur.fetchone()[0]
        conn.commit()

    def error(self, etapa: str, clave: str, mensaje: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ctl.error (ejecucion_id, etapa, clave, mensaje) VALUES (%s,%s,%s,%s)",
                (self.id, etapa, str(clave), (mensaje or "")[:2000]),
            )

    def resolver(self, etapa: str, clave: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE ctl.error SET resuelto_en = now() WHERE etapa=%s AND clave=%s AND resuelto_en IS NULL",
                (etapa, str(clave)),
            )

    def sumar(self, clave: str, n: int = 1) -> None:
        self.resumen[clave] = self.resumen.get(clave, 0) + n

    def cerrar(self, ok: bool) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE ctl.ejecucion SET fin = now(), ok = %s, resumen = %s WHERE id = %s",
                (ok, json.dumps(self.resumen, default=str), self.id),
            )
        self.conn.commit()


@contextmanager
def ejecucion(conn: psycopg.Connection, comando: str, parametros: dict | None = None) -> Iterator[Ejecucion]:
    e = Ejecucion(conn, comando, parametros)
    try:
        yield e
    except BaseException:
        conn.rollback()
        e.cerrar(False)
        raise
    else:
        e.cerrar(True)
