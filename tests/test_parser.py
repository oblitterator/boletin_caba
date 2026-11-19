import json
from pathlib import Path

import pytest

from monitor_licitaciones.parser_actos import parsear_acto, clave_firma, a_numero, cuit_valido

_FIX = Path(__file__).parent / "fixtures"
# actos.json: casos con los que se diseñaron las reglas.
# actos_holdout*.json: muestras aleatorias posteriores (miden generalización; luego quedan como regresión).
CASOS = [c for f in ("actos.json", "actos_holdout.json", "actos_holdout2.json", "actos_reales_v2.json")
         for c in json.loads((_FIX / f).read_text(encoding="utf-8"))]


@pytest.mark.parametrize("caso", CASOS, ids=[str(c["id_norma"]) for c in CASOS])
def test_acto(caso):
    a = parsear_acto(caso["nombre"], caso["texto"])
    esp = caso["esperado"]

    for campo in ("codigo_proceso", "tipo_proc", "etapa", "n_ofertas", "unica_oferta", "etapa_conflicto", "fracasada", "moneda"):
        if campo in esp:
            assert getattr(a, campo) == esp[campo], f"{campo}: {getattr(a, campo)!r} != {esp[campo]!r}"
    if esp["monto_total"] is None:
        assert a.monto_total is None, f"monto_total {a.monto_total}"
    else:
        assert a.monto_total == pytest.approx(esp["monto_total"]), f"monto_total {a.monto_total}"

    obtenidas = {clave_firma(f.nombre): f for f in a.firmas}
    for fe in esp["firmas"]:
        k = clave_firma(fe["nombre"])
        assert k in obtenidas, f"falta firma {fe['nombre']!r}; obtenidas={[f.nombre for f in a.firmas]}"
        f = obtenidas[k]
        assert f.rol == fe["rol"], f"{fe['nombre']}: rol {f.rol} != {fe['rol']}"
        if "cuit" in fe:
            assert f.cuit_texto == fe["cuit"]
        if "monto" in fe:
            assert f.monto == pytest.approx(fe["monto"])
    # no debe haber firmas de más
    esperadas = {clave_firma(fe["nombre"]) for fe in esp["firmas"]}
    sobrantes = [f.nombre for f in a.firmas if clave_firma(f.nombre) not in esperadas]
    assert not sobrantes, f"firmas sobrantes: {sobrantes}"


@pytest.mark.parametrize("txt,val", [
    ("5.426.800.-", 5426800), ("4.228.180 ,00", 4228180.0), ("2967,16", 2967.16),
    ("13.500-", 13500), ("6.006.000.", 6006000), ("1110", 1110), ("27.982.960,00", 27982960.0),
])
def test_a_numero(txt, val):
    assert a_numero(txt) == pytest.approx(val)


def test_cuit_valido():
    assert cuit_valido("30563006664")
    assert not cuit_valido("30563006665")
