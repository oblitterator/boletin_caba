"""Cliente de la API pública del Boletín Oficial CABA.

https://api-restboletinoficial.buenosaires.gob.ar/obtenerBoletin/{dd-mm-aaaa}/true

Estructura de respuesta (la misma que usaba la v1):
  {"boletin": {"numero": ..., "fecha_publicacion": "dd/mm/aaaa", ...},
   "normas":  {"normas": {seccion: {tipo: {organismo: [norma, ...]}}}}
              | {"errores": ["..."]}}
Para fechas sin edición la API puede devolver 404 o el boletín más cercano; por eso
siempre se usa la fecha que trae el propio boletín, no la consultada.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api-restboletinoficial.buenosaires.gob.ar"
UA = "monitor-licitaciones-caba/2 (+analisis de compras publicas; contacto en README)"


class ErrorAPI(Exception):
    pass


def sesion(reintentos: int = 3) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=reintentos, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",), raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = UA
    return s


@dataclass
class RespuestaBoletin:
    fecha_consultada: date
    boletin: Optional[dict]          # None = no hay boletín para esa fecha
    normas: list[dict]               # aplanadas, con seccion/tipo/organismo
    error_normas: Optional[str] = None

    @property
    def fecha_publicacion(self) -> Optional[date]:
        if not self.boletin:
            return None
        return parsear_fecha(self.boletin.get("fecha_publicacion"))


def parsear_fecha(s: Any) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip()[:10]
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def aplanar_normas(normas_data: Any) -> Iterator[dict]:
    """{seccion: {tipo: {organismo: [norma]}}} -> filas con seccion/tipo/organismo."""
    if not isinstance(normas_data, dict):
        return
    for seccion, tipos in normas_data.items():
        if not isinstance(tipos, dict):
            continue
        for tipo, organismos in tipos.items():
            if not isinstance(organismos, dict):
                continue
            for organismo, lista in organismos.items():
                for n in lista or []:
                    if isinstance(n, dict):
                        yield {"_seccion": seccion, "_tipo": tipo, "_organismo": organismo, **n}


class BoletinAPI:
    def __init__(self, session: Optional[requests.Session] = None, timeout: float = 60, pausa: float = 0.5):
        self.s = session or sesion()
        self.timeout = timeout
        self.pausa = pausa

    def obtener_boletin(self, fecha: date) -> RespuestaBoletin:
        url = f"{BASE_URL}/obtenerBoletin/{fecha:%d-%m-%Y}/true"
        try:
            r = self.s.get(url, timeout=(10, self.timeout))
        except requests.RequestException as e:
            raise ErrorAPI(f"{type(e).__name__}: {e}") from e
        finally:
            time.sleep(self.pausa)
        if r.status_code == 404:
            return RespuestaBoletin(fecha, None, [])
        if r.status_code >= 400:
            raise ErrorAPI(f"HTTP {r.status_code}")
        try:
            data = r.json()
        except ValueError as e:
            raise ErrorAPI("respuesta no es JSON") from e
        return interpretar_respuesta(fecha, data)


def interpretar_respuesta(fecha: date, data: Any) -> RespuestaBoletin:
    if not isinstance(data, dict) or not isinstance(data.get("boletin"), dict) or not data["boletin"].get("numero"):
        return RespuestaBoletin(fecha, None, [])
    boletin = {k: v for k, v in data["boletin"].items() if not (isinstance(k, str) and k.isnumeric())}
    nd = data.get("normas") or {}
    if isinstance(nd, dict) and nd.get("errores"):
        errores = nd["errores"]
        return RespuestaBoletin(fecha, boletin, [], error_normas=str(errores[0] if errores else "error en normas"))
    normas = list(aplanar_normas(nd.get("normas") if isinstance(nd, dict) else None))
    return RespuestaBoletin(fecha, boletin, normas)
