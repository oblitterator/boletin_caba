"""Genera los notebooks desde código (así quedan versionables y revisables en git).
Uso: python notebooks/_construir.py"""
from pathlib import Path

import nbformat as nbf

AQUI = Path(__file__).parent


def nb(celdas):
    n = nbf.v4.new_notebook()
    n.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
    n.cells = [nbf.v4.new_markdown_cell(c[3:]) if c.startswith("md:") else nbf.v4.new_code_cell(c.strip())
               for c in celdas]
    return n


SETUP = """
import os
import pandas as pd
import matplotlib.pyplot as plt
from monitor_licitaciones.consultas import Base

pd.set_option("display.max_colwidth", 120)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
db = Base()   # usa MLC_DSN; conexión de solo lectura
print(db.valor("SELECT current_database()"), "| parser", db.valor("SELECT max(parser_version) FROM bo.acto"))
"""

# ---------------------------------------------------------------------------
CALIDAD = [
"""md:# 01 · Calidad de la extracción

Antes de mirar señales hay que saber **cuánto se puede confiar en los datos**. Este notebook mide:

1. Cobertura de textos (PDFs bajados vs. pendientes)
2. Calidad del parser por etapa y año
3. Resolución de CUIT y la lista priorizada de pendientes
4. Inconsistencias de montos
5. **Muestra aleatoria para revisión manual**: la única forma de conocer la precisión real

Solo lectura: no modifica la base.""",
SETUP,
"""md:## 1. Cobertura de textos
`pendiente` = normas de la sección Licitaciones cuyo PDF todavía no se bajó (`python -m monitor_licitaciones pdfs`).""",
"""
doc = db.df("SELECT anio, estado, documentos FROM analitica.calidad_documentos ORDER BY anio")
tabla = doc.pivot_table(index="anio", columns="estado", values="documentos", aggfunc="sum", fill_value=0)
tabla["% con texto"] = (100 * tabla.get("ok", 0) / tabla.sum(axis=1)).round(1)
display(tabla)
tabla.drop(columns="% con texto").plot.bar(stacked=True, figsize=(9, 3.5), title="Documentos por año y estado")
plt.ylabel("documentos"); plt.tight_layout(); plt.show()
""",
"""md:## 2. Parser por etapa y año
Lo que importa para las señales son **adjudicación** y **preadjudicación**: código, monto y firma ganadora.""",
"""
cp = db.df("SELECT * FROM analitica.calidad_parser WHERE etapa IN ('adjudicacion','preadjudicacion') ORDER BY anio, etapa")
display(cp)
fig, ax = plt.subplots(1, 3, figsize=(13, 3.5), sharey=True)
for i, col in enumerate(["pct_alta", "pct_con_monto", "pct_con_codigo"]):
    cp.pivot(index="anio", columns="etapa", values=col).plot(ax=ax[i], marker="o", title=col)
    ax[i].set_ylim(0, 100)
plt.tight_layout(); plt.show()
""",
"""
# ¿Qué falta en los actos que no son de confianza alta?
db.df(\"\"\"SELECT etapa, faltantes, count(*) AS actos FROM bo.acto
          WHERE etapa IN ('adjudicacion','preadjudicacion') AND confianza <> 'alta'
          GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15\"\"\")
""",
"""md:## 3. Resolución de CUIT
Orden de confianza: `cuit_en_texto` > `alias` > `padron_exacto` > `padron_base` > `padron_trgm`.
Lo `sin_resolver` incluye personas humanas (no están en el padrón de sociedades) y **homónimos**: el padrón tiene varias
sociedades con el mismo nombre y la regla no adivina.""",
"""
res = db.df("SELECT * FROM analitica.calidad_resolucion")
piv = res.pivot_table(index="rol", columns="resolucion", values="firmas", fill_value=0)
display(piv)
(piv.div(piv.sum(axis=1), axis=0) * 100).plot.barh(stacked=True, figsize=(10, 3.5), title="% de firmas por método de resolución")
plt.xlabel("%"); plt.legend(bbox_to_anchor=(1, 1)); plt.tight_layout(); plt.show()
""",
"""md:### Pendientes priorizados
Los nombres sin CUIT con más actos. Si reconocés el candidato correcto, cargalo como alias **manual** (tiene prioridad sobre
todo lo automático) y volvé a correr `python -m monitor_licitaciones resolver`.

```sql
INSERT INTO ref.empresa_alias (nombre_norm, cuit, fuente) VALUES ('GBO ARGENTINA SA', 30708003391, 'manual');
```""",
"""
db.df(\"\"\"SELECT p.*, ref.candidatos_cuit(p.ejemplo) AS candidatos
          FROM (SELECT * FROM analitica.pendientes_cuit ORDER BY actos DESC LIMIT 40) p
          ORDER BY p.actos DESC\"\"\")
""",
"""md:## 4. Montos que no cierran
Actos donde la suma de montos por firma difiere más de 1% del total declarado. Pueden ser errores del propio Boletín o del
parser: conviene abrir el PDF (`url_norma`).""",
"""
cm = db.df("SELECT * FROM analitica.control_montos ORDER BY abs(pct_diferencia) DESC")
print(len(cm), "actos con diferencia > 1%")
cm.head(25)
""",
"""md:## 5. Muestra para revisión manual
Genera un CSV con una muestra aleatoria **reproducible** de adjudicaciones y preadjudicaciones. Abrilo, compará cada fila
con el texto y completá las columnas `ok_*` con 1 (bien) o 0 (mal). Con 100 casos el error de la estimación ronda ±10 pp.""",
"""
N, SEMILLA = 100, "revision-1"   # misma semilla = misma muestra
muestra = db.df(\"\"\"
  SELECT a.id_norma, a.etapa, a.codigo_proceso, a.monto_total, a.moneda, a.n_ofertas, a.unica_oferta,
         (SELECT string_agg(af.nombre_raw || ' [' || af.rol || coalesce(', ' || af.cuit, '') ||
                 coalesce(', $' || af.monto, '') || ']', ' | ' ORDER BY af.orden)
            FROM bo.acto_firma af WHERE af.id_norma = a.id_norma) AS firmas,
         n.url_norma, left(d.texto, 3000) AS texto,
         NULL::int AS ok_codigo, NULL::int AS ok_monto, NULL::int AS ok_ganadoras, NULL::int AS ok_sin_basura,
         NULL::text AS comentario
  FROM bo.acto a JOIN bo.norma n USING (id_norma) JOIN bo.documento d USING (id_norma)
  WHERE a.etapa IN ('adjudicacion','preadjudicacion')
  ORDER BY md5(a.id_norma::text || %s) LIMIT %s\"\"\", (SEMILLA, N))
ruta = os.path.join("..", "revision_manual.csv")
if not os.path.exists(ruta):
    muestra.to_csv(ruta, index=False, encoding="utf-8-sig")
    print("Muestra guardada en", os.path.abspath(ruta))
else:
    print("Ya existe", os.path.abspath(ruta), "— no se pisa (borralo para generar otra)")
muestra[["id_norma", "etapa", "codigo_proceso", "monto_total", "firmas"]].head(10)
""",
"""
# Después de completar el CSV: precisión estimada con intervalo de confianza 95% (Wilson)
import math
def wilson(k, n, z=1.96):
    if n == 0: return (float("nan"),) * 3
    p = k / n; d = 1 + z*z/n
    c = (p + z*z/(2*n)) / d; m = z * math.sqrt(p*(1-p)/n + z*z/(4*n*n)) / d
    return round(100*p, 1), round(100*(c-m), 1), round(100*(c+m), 1)

if os.path.exists(ruta):
    rev = pd.read_csv(ruta)
    cols = [c for c in rev.columns if c.startswith("ok_")]
    filas = []
    for c in cols:
        v = rev[c].dropna()
        filas.append((c, len(v), *wilson(int(v.sum()), len(v))))
    display(pd.DataFrame(filas, columns=["campo", "revisados", "% correcto", "IC95 inf", "IC95 sup"]))
""",
]

# ---------------------------------------------------------------------------
SENALES = [
"""md:# 02 · Señales de contratación

Indicadores para **priorizar revisión**. Ninguno prueba irregularidad: hay razones legítimas para una oferta única, una
contratación directa o un proveedor dominante. Mirá primero `01_calidad`.

Montos: nominales salvo que cargues IPC (`python -m monitor_licitaciones ipc --csv ...`). Solo ARS.""",
SETUP,
"""
hay_ipc = db.valor("SELECT count(*) FROM ref.ipc") > 0
print("Pesos constantes:", "sí" if hay_ipc else "NO — los montos de distintos años no son comparables")
""",
"""md:## Panorama""",
"""
pan = db.df(\"\"\"SELECT anio, count(DISTINCT codigo_proceso) AS procesos, count(DISTINCT cuit) AS proveedores,
                     sum(monto_asignado) FILTER (WHERE moneda='ARS') AS monto_nominal,
                     sum(monto_constante) AS monto_constante
              FROM analitica.ganador GROUP BY anio ORDER BY anio\"\"\")
display(pan)
pan.plot(x="anio", y=["procesos", "proveedores"], marker="o", figsize=(8, 3), title="Procesos y proveedores por año")
plt.tight_layout(); plt.show()
""",
"""md:## 1. Competencia: oferta única
Solo organismos con al menos 20 procesos con dato. El dato de cantidad de ofertas aparece en una minoría de los textos:
mirá `procesos_con_dato`.""",
"""
comp = db.df(\"\"\"SELECT organismo, anio, procesos, procesos_con_dato, oferta_unica, pct_oferta_unica, promedio_ofertas
               FROM analitica.s_competencia WHERE procesos_con_dato >= 20
               ORDER BY pct_oferta_unica DESC NULLS LAST\"\"\")
comp.head(20)
""",
"""md:## 2. Concentración de proveedores (HHI)
HHI: 0 = competencia atomizada; > 2.500 = alta concentración (umbral usual de defensa de la competencia).
Se excluyen organismo-año con menos de 5 proveedores para no sobreinterpretar.""",
"""
conc = db.df(\"\"\"SELECT c.*, e.razon_social AS proveedor_principal
               FROM analitica.s_concentracion c LEFT JOIN ref.empresa e ON e.cuit = c.cuit_principal
               WHERE c.proveedores >= 5 ORDER BY c.hhi DESC\"\"\")
display(conc.head(20))
if len(conc):
    conc["hhi"].astype(float).plot.hist(bins=30, figsize=(8, 3), title="Distribución del HHI (organismo-año)")
    plt.axvline(2500, ls="--", c="k"); plt.tight_layout(); plt.show()
""",
"""md:## 3. Procedimientos de excepción
Peso de contrataciones directas (CDI) y menores (CME) por organismo.""",
"""
proc = db.df(\"\"\"SELECT organismo, anio, procesos, directas, menores, pct_excepcion_cantidad, pct_excepcion_monto_nominal
               FROM analitica.s_procedimiento WHERE procesos >= 20 ORDER BY pct_excepcion_cantidad DESC\"\"\")
proc.head(20)
""",
"""md:## 4. Posible fraccionamiento
3+ contrataciones menores/directas al mismo proveedor, mismo organismo, en 90 días. `supera_tope_acumulado` solo se
calcula si cargaste los topes en `ref.umbral_procedimiento`.""",
"""
fr = db.df(\"\"\"SELECT f.organismo, f.cuit, e.razon_social, max(f.n_90d) AS max_en_90_dias,
                     sum(f.monto_asignado) AS monto_total, bool_or(f.supera_tope_acumulado) AS supera_tope,
                     min(f.fecha) AS desde, max(f.fecha) AS hasta
              FROM analitica.s_fraccionamiento f LEFT JOIN ref.empresa e ON e.cuit = f.cuit
              GROUP BY 1, 2, 3 ORDER BY max_en_90_dias DESC, monto_total DESC NULLS LAST\"\"\")
fr.head(25)
""",
"""md:## 5. Empresas recién constituidas al ganar""",
"""
db.df("SELECT * FROM analitica.s_empresa_nueva ORDER BY dias_desde_constitucion, monto_asignado DESC NULLS LAST")
""",
"""md:## 6. Domicilio fiscal compartido en el mismo acto
Depende de que ambas firmas tengan CUIT resuelto y domicilio en el padrón.""",
"""
db.df("SELECT * FROM analitica.s_domicilio_compartido ORDER BY fecha_publicacion DESC")
""",
"""md:## 7. Prórrogas, ampliaciones y modificaciones""",
"""
db.df("SELECT * FROM analitica.s_modificaciones ORDER BY n_prorrogas + n_ampliaciones + n_modificaciones DESC LIMIT 25")
""",
"""md:## 8. Proveedores para mirar primero
Cuenta en cuántas señales aparece cada proveedor. Es un **orden de revisión**, no un puntaje de riesgo.""",
"""
prov = db.df(\"\"\"
WITH s AS (
  SELECT cuit, 'fraccionamiento' AS senal FROM analitica.s_fraccionamiento
  UNION SELECT cuit, 'empresa_nueva' FROM analitica.s_empresa_nueva
  UNION SELECT cuit_1, 'domicilio_compartido' FROM analitica.s_domicilio_compartido
  UNION SELECT cuit_2, 'domicilio_compartido' FROM analitica.s_domicilio_compartido
  UNION SELECT cuit_principal, 'principal_en_organismo_concentrado' FROM analitica.s_concentracion WHERE hhi > 2500 AND proveedores >= 5
)
SELECT p.cuit, p.razon_social, p.procesos_ganados, p.organismos, p.monto_nominal_ars, p.pct_oferta_unica, p.pct_excepcion,
       count(DISTINCT s.senal) AS n_senales, string_agg(DISTINCT s.senal, ', ') AS senales
FROM analitica.proveedor p JOIN s USING (cuit)
GROUP BY p.cuit, p.razon_social, p.procesos_ganados, p.organismos, p.monto_nominal_ars, p.pct_oferta_unica, p.pct_excepcion
ORDER BY n_senales DESC, p.monto_nominal_ars DESC NULLS LAST\"\"\")
prov.head(30)
""",
]

nbf.write(nb(CALIDAD), AQUI / "01_calidad.ipynb")
nbf.write(nb(SENALES), AQUI / "02_senales.ipynb")
print("ok")
