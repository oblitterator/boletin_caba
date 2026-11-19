-- =====================================================================
-- Vistas analíticas y SEÑALES.
-- Las señales son indicadores para priorizar revisión, NO prueban irregularidad.
-- El esquema analitica se recrea entero en cada init-db: NO guardar objetos
-- propios ahí (usar otro esquema para vistas personales).
-- =====================================================================
DROP SCHEMA IF EXISTS analitica CASCADE;
CREATE SCHEMA analitica;

-- Acto + datos de la norma
CREATE VIEW analitica.acto_v AS
SELECT a.*, n.fecha_publicacion, n.organismo, n.seccion, n.tipo AS tipo_norma,
       n.nombre AS nombre_norma, n.url_norma, n.boletin_numero,
       coalesce(a.uoa::text, n.organismo) AS organismo_id
FROM bo.acto a JOIN bo.norma n USING (id_norma);

-- Factor para pasar pesos corrientes a pesos del último mes con IPC cargado
CREATE VIEW analitica.ipc_factor AS
SELECT i.mes, (SELECT indice FROM ref.ipc ORDER BY mes DESC LIMIT 1) / i.indice AS factor
FROM ref.ipc i;

-- ---------------------------------------------------------------------
-- Un registro por proceso de compra (código BAC o SIGAF)
-- ---------------------------------------------------------------------
CREATE VIEW analitica.proceso AS
WITH a AS (SELECT * FROM analitica.acto_v WHERE codigo_proceso IS NOT NULL)
SELECT
  codigo_proceso,
  max(uoa)                                   AS uoa,
  max(tipo_proc)                             AS tipo_proc,
  max(anio_proc)                             AS anio_proc,
  mode() WITHIN GROUP (ORDER BY organismo)   AS organismo,
  mode() WITHIN GROUP (ORDER BY organismo_id) AS organismo_id,
  (array_agg(objeto ORDER BY fecha_publicacion) FILTER (WHERE objeto IS NOT NULL))[1] AS objeto,
  min(fecha_publicacion) FILTER (WHERE etapa = 'llamado')          AS fecha_llamado,
  max(fecha_publicacion) FILTER (WHERE etapa = 'preadjudicacion')  AS fecha_preadjudicacion,
  max(fecha_publicacion) FILTER (WHERE etapa = 'adjudicacion')     AS fecha_adjudicacion,
  (array_agg(monto_total ORDER BY fecha_publicacion DESC, id_norma DESC)
     FILTER (WHERE etapa = 'adjudicacion' AND monto_total IS NOT NULL))[1]    AS monto_adjudicado,
  (array_agg(moneda ORDER BY fecha_publicacion DESC, id_norma DESC)
     FILTER (WHERE etapa = 'adjudicacion' AND monto_total IS NOT NULL))[1]    AS moneda_adjudicado,
  (array_agg(monto_total ORDER BY fecha_publicacion DESC, id_norma DESC)
     FILTER (WHERE etapa = 'preadjudicacion' AND monto_total IS NOT NULL))[1] AS monto_preadjudicado,
  (array_agg(moneda ORDER BY fecha_publicacion DESC, id_norma DESC)
     FILTER (WHERE etapa = 'preadjudicacion' AND monto_total IS NOT NULL))[1] AS moneda_preadjudicado,
  max(n_ofertas)                             AS n_ofertas,
  max(n_firmas_identificadas)                AS n_firmas_identificadas,
  bool_or(unica_oferta)                      AS unica_oferta,
  bool_or(fracasada)                         AS fracasada,
  count(*) FILTER (WHERE etapa = 'prorroga')                 AS n_prorrogas,
  count(*) FILTER (WHERE etapa = 'ampliacion')               AS n_ampliaciones,
  count(*) FILTER (WHERE etapa LIKE 'circular%')             AS n_circulares,
  count(*) FILTER (WHERE etapa IN ('modificacion','rectificacion')) AS n_modificaciones,
  count(*)                                   AS n_actos
FROM a
GROUP BY codigo_proceso;

-- ---------------------------------------------------------------------
-- Ganadores por proceso, tomados del acto final (adjudicación; si no hay,
-- preadjudicación). El monto se asigna a la firma SOLO si es inequívoco:
-- una sola ganadora, o el texto da el monto de esa firma. Nunca se duplica.
-- ---------------------------------------------------------------------
CREATE VIEW analitica.ganador AS
WITH final AS (
  SELECT DISTINCT ON (codigo_proceso)
         codigo_proceso, id_norma, etapa, fecha_publicacion, monto_total, moneda, organismo, organismo_id,
         tipo_proc, uoa
  FROM analitica.acto_v
  WHERE codigo_proceso IS NOT NULL AND etapa IN ('adjudicacion','preadjudicacion') AND NOT fracasada
  ORDER BY codigo_proceso, (etapa = 'adjudicacion') DESC, fecha_publicacion DESC, id_norma DESC
), g AS (
  SELECT f.*, af.orden, af.nombre_raw, af.nombre_norm, af.cuit, af.resolucion, af.score,
         af.monto AS monto_firma,
         count(*) OVER (PARTITION BY f.id_norma) AS n_ganadoras
  FROM final f
  JOIN bo.acto_firma af ON af.id_norma = f.id_norma AND af.rol IN ('adjudicada','preadjudicada')
)
SELECT g.*,
       CASE WHEN n_ganadoras = 1 THEN monto_total ELSE monto_firma END AS monto_asignado,
       (n_ganadoras > 1 AND monto_firma IS NULL)                          AS monto_no_asignable,
       CASE WHEN moneda = 'ARS' THEN
            (CASE WHEN n_ganadoras = 1 THEN monto_total ELSE monto_firma END) * fx.factor END AS monto_constante,
       extract(year FROM fecha_publicacion)::int AS anio
FROM g
LEFT JOIN analitica.ipc_factor fx ON fx.mes = date_trunc('month', g.fecha_publicacion)::date;

-- =====================================================================
-- SEÑALES
-- =====================================================================

-- 1) Competencia: procesos con oferta única, por organismo y año
CREATE VIEW analitica.s_competencia AS
SELECT organismo_id, mode() WITHIN GROUP (ORDER BY organismo) AS organismo,
       extract(year FROM coalesce(fecha_adjudicacion, fecha_preadjudicacion))::int AS anio,
       count(*)                                              AS procesos,
       count(*) FILTER (WHERE unica_oferta IS NOT NULL)      AS procesos_con_dato,
       count(*) FILTER (WHERE unica_oferta)                  AS oferta_unica,
       round(100.0 * count(*) FILTER (WHERE unica_oferta)
             / nullif(count(*) FILTER (WHERE unica_oferta IS NOT NULL), 0), 1) AS pct_oferta_unica,
       round(avg(n_ofertas), 2)                              AS promedio_ofertas
FROM analitica.proceso
WHERE coalesce(fecha_adjudicacion, fecha_preadjudicacion) IS NOT NULL
GROUP BY 1, 3;

-- 2) Concentración de proveedores (HHI 0–10.000) por organismo y año.
--    Usa pesos constantes si hay IPC; si no, nominales.
CREATE VIEW analitica.s_concentracion AS
WITH base AS (
  SELECT organismo_id, organismo, anio, cuit,
         sum(coalesce(monto_constante, monto_asignado)) AS monto
  FROM analitica.ganador
  WHERE cuit IS NOT NULL AND moneda = 'ARS' AND monto_asignado IS NOT NULL
  GROUP BY 1, 2, 3, 4
), tot AS (
  SELECT organismo_id, anio, sum(monto) AS total, count(*) AS proveedores FROM base GROUP BY 1, 2
), sh AS (
  SELECT b.*, t.total, t.proveedores, b.monto / nullif(t.total, 0) AS share,
         row_number() OVER (PARTITION BY b.organismo_id, b.anio ORDER BY b.monto DESC) AS rk
  FROM base b JOIN tot t USING (organismo_id, anio)
)
SELECT organismo_id, max(organismo) AS organismo, anio, max(total) AS monto_total, max(proveedores) AS proveedores,
       round(sum(share * share) * 10000) AS hhi,
       max(cuit) FILTER (WHERE rk = 1)   AS cuit_principal,
       round(100 * max(share) , 1)       AS pct_principal,
       (SELECT count(*) FROM ref.ipc) > 0 AS en_pesos_constantes
FROM sh GROUP BY organismo_id, anio;

-- 3) Uso de procedimientos de excepción (contratación menor y directa)
CREATE VIEW analitica.s_procedimiento AS
SELECT p.organismo_id, mode() WITHIN GROUP (ORDER BY p.organismo) AS organismo,
       extract(year FROM coalesce(p.fecha_adjudicacion, p.fecha_preadjudicacion))::int AS anio,
       count(*) AS procesos,
       count(*) FILTER (WHERE p.tipo_proc = 'CDI') AS directas,
       count(*) FILTER (WHERE p.tipo_proc = 'CME') AS menores,
       round(100.0 * count(*) FILTER (WHERE p.tipo_proc IN ('CDI','CME')) / count(*), 1) AS pct_excepcion_cantidad,
       round(100.0 * sum(coalesce(p.monto_adjudicado, p.monto_preadjudicado)) FILTER (WHERE p.tipo_proc IN ('CDI','CME'))
             / nullif(sum(coalesce(p.monto_adjudicado, p.monto_preadjudicado)), 0), 1) AS pct_excepcion_monto_nominal
FROM analitica.proceso p
WHERE coalesce(p.fecha_adjudicacion, p.fecha_preadjudicacion) IS NOT NULL
GROUP BY 1, 3;

-- 4) Posible fraccionamiento: 3+ contrataciones menores/directas al mismo proveedor,
--    en el mismo organismo, dentro de 90 días. Si hay topes cargados en
--    ref.umbral_procedimiento, marca cuando la suma supera el tope y cada una no.
CREATE VIEW analitica.s_fraccionamiento AS
WITH g AS (
  SELECT organismo_id, organismo, cuit, codigo_proceso, tipo_proc, fecha_publicacion::date AS fecha, monto_asignado
  FROM analitica.ganador
  WHERE tipo_proc IN ('CME','CDI') AND cuit IS NOT NULL AND moneda = 'ARS'
), w AS (
  SELECT g.*,
         count(*)            OVER v AS n_90d,
         sum(monto_asignado) OVER v AS suma_90d,
         max(monto_asignado) OVER v AS max_90d
  FROM g
  WINDOW v AS (PARTITION BY organismo_id, cuit ORDER BY fecha RANGE BETWEEN INTERVAL '90 days' PRECEDING AND CURRENT ROW)
)
SELECT w.*, u.monto_tope,
       (u.monto_tope IS NOT NULL AND w.suma_90d > u.monto_tope AND w.max_90d <= u.monto_tope) AS supera_tope_acumulado
FROM w
LEFT JOIN LATERAL (
  SELECT monto_tope FROM ref.umbral_procedimiento r
  WHERE r.tipo_proc = w.tipo_proc AND r.vigente_desde <= w.fecha
  ORDER BY r.vigente_desde DESC LIMIT 1
) u ON true
WHERE w.n_90d >= 3;

-- 5) Empresas recién constituidas al ganar (< 365 días desde el contrato social)
CREATE VIEW analitica.s_empresa_nueva AS
SELECT g.codigo_proceso, g.organismo, g.fecha_publicacion, g.cuit, e.razon_social, e.tipo_societario,
       e.fecha_contrato_social, (g.fecha_publicacion - e.fecha_contrato_social) AS dias_desde_constitucion,
       g.monto_asignado, g.moneda
FROM analitica.ganador g JOIN ref.empresa e ON e.cuit = g.cuit
WHERE e.fecha_contrato_social IS NOT NULL
  AND g.fecha_publicacion - e.fecha_contrato_social BETWEEN 0 AND 364;

-- 6) Firmas distintas con el mismo domicilio fiscal que aparecen en el mismo acto
--    (como oferentes, desestimadas o ganadoras). Señal clásica de colusión.
CREATE VIEW analitica.s_domicilio_compartido AS
SELECT a.codigo_proceso, a.organismo, a.fecha_publicacion,
       x.cuit AS cuit_1, ex.razon_social AS firma_1, x.rol AS rol_1,
       y.cuit AS cuit_2, ey.razon_social AS firma_2, y.rol AS rol_2,
       ex.dom_fiscal AS domicilio
FROM bo.acto_firma x
JOIN bo.acto_firma y ON y.id_norma = x.id_norma AND x.cuit < y.cuit
JOIN ref.empresa ex ON ex.cuit = x.cuit
JOIN ref.empresa ey ON ey.cuit = y.cuit
JOIN analitica.acto_v a ON a.id_norma = x.id_norma
WHERE ex.dom_fiscal_norm IS NOT NULL AND length(ex.dom_fiscal_norm) >= 8
  AND ex.dom_fiscal_norm = ey.dom_fiscal_norm;

-- 7) Procesos con prórrogas, ampliaciones o modificaciones posteriores
CREATE VIEW analitica.s_modificaciones AS
SELECT codigo_proceso, organismo, tipo_proc, objeto, fecha_adjudicacion, monto_adjudicado, moneda_adjudicado,
       n_prorrogas, n_ampliaciones, n_modificaciones, n_circulares
FROM analitica.proceso
WHERE n_prorrogas + n_ampliaciones + n_modificaciones > 0;

-- ---------------------------------------------------------------------
-- Resumen por proveedor (para el tablero)
-- ---------------------------------------------------------------------
CREATE VIEW analitica.proveedor AS
SELECT g.cuit, e.razon_social, e.tipo_societario, e.fecha_contrato_social,
       count(DISTINCT g.codigo_proceso)                         AS procesos_ganados,
       count(DISTINCT g.organismo_id)                           AS organismos,
       sum(g.monto_asignado)    FILTER (WHERE g.moneda = 'ARS') AS monto_nominal_ars,
       sum(g.monto_constante)                                   AS monto_constante_ars,
       count(*) FILTER (WHERE g.monto_no_asignable)             AS procesos_monto_no_asignable,
       round(100.0 * count(*) FILTER (WHERE p.unica_oferta)
             / nullif(count(*) FILTER (WHERE p.unica_oferta IS NOT NULL), 0), 1) AS pct_oferta_unica,
       round(100.0 * count(*) FILTER (WHERE g.tipo_proc IN ('CDI','CME')) / count(*), 1) AS pct_excepcion,
       min(g.fecha_publicacion) AS primera, max(g.fecha_publicacion) AS ultima
FROM analitica.ganador g
LEFT JOIN ref.empresa e ON e.cuit = g.cuit
LEFT JOIN analitica.proceso p ON p.codigo_proceso = g.codigo_proceso
WHERE g.cuit IS NOT NULL
GROUP BY g.cuit, e.razon_social, e.tipo_societario, e.fecha_contrato_social;

-- ---------------------------------------------------------------------
-- Calidad de la extracción (mirar ANTES de confiar en las señales)
-- ---------------------------------------------------------------------
CREATE VIEW analitica.calidad_parser AS
SELECT extract(year FROM fecha_publicacion)::int AS anio, etapa,
       count(*) AS actos,
       round(100.0 * count(*) FILTER (WHERE confianza = 'alta')  / count(*), 1) AS pct_alta,
       round(100.0 * count(*) FILTER (WHERE confianza = 'baja')  / count(*), 1) AS pct_baja,
       round(100.0 * count(*) FILTER (WHERE codigo_proceso IS NOT NULL) / count(*), 1) AS pct_con_codigo,
       round(100.0 * count(*) FILTER (WHERE monto_total IS NOT NULL)    / count(*), 1) AS pct_con_monto,
       round(100.0 * count(*) FILTER (WHERE etapa_conflicto)            / count(*), 1) AS pct_etapa_conflicto
FROM analitica.acto_v
GROUP BY 1, 2;

CREATE VIEW analitica.calidad_resolucion AS
SELECT rol, coalesce(resolucion, 'sin_correr') AS resolucion, count(*) AS firmas,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY rol), 1) AS pct_del_rol
FROM bo.acto_firma GROUP BY 1, 2;

CREATE VIEW analitica.calidad_documentos AS
SELECT d.estado, extract(year FROM n.fecha_publicacion)::int AS anio, count(*) AS documentos,
       count(*) FILTER (WHERE d.intentos >= 3) AS agotaron_reintentos
FROM bo.documento d JOIN bo.norma n USING (id_norma) GROUP BY 1, 2;

-- Actos donde la suma de montos por firma no coincide con el total declarado
-- (errores de tipeo del propio Boletín o del parser: revisar a mano)
CREATE VIEW analitica.control_montos AS
SELECT a.id_norma, a.codigo_proceso, a.etapa, a.fecha_publicacion, a.url_norma, a.monto_total,
       sum(af.monto) AS suma_firmas, count(af.monto) AS firmas_con_monto,
       round(100 * (sum(af.monto) - a.monto_total) / nullif(a.monto_total, 0), 2) AS pct_diferencia
FROM analitica.acto_v a
JOIN bo.acto_firma af ON af.id_norma = a.id_norma AND af.rol IN ('adjudicada','preadjudicada')
WHERE a.monto_total IS NOT NULL
GROUP BY a.id_norma, a.codigo_proceso, a.etapa, a.fecha_publicacion, a.url_norma, a.monto_total
HAVING count(af.monto) = count(*) AND count(*) > 1
   AND abs(sum(af.monto) - a.monto_total) > 0.01 * a.monto_total;

-- Firmas ganadoras sin CUIT, ordenadas por impacto, con candidatos del padrón.
-- Para resolverlas: INSERT INTO ref.empresa_alias (nombre_norm, cuit, fuente) VALUES ('<nombre_norm>', <cuit>, 'manual');
CREATE VIEW analitica.pendientes_cuit AS
WITH p AS (
  SELECT af.nombre_norm, min(af.nombre_raw) AS ejemplo, count(*) AS actos,
         count(DISTINCT a.organismo_id) AS organismos,
         sum(coalesce(af.monto, CASE WHEN a.n_firmas_identificadas = 1 THEN a.monto_total END)) AS monto_aprox,
         max(a.fecha_publicacion) AS ultima
  FROM bo.acto_firma af JOIN analitica.acto_v a USING (id_norma)
  WHERE af.resolucion = 'sin_resolver' AND af.rol IN ('adjudicada','preadjudicada') AND af.nombre_norm IS NOT NULL
  GROUP BY af.nombre_norm
)
SELECT p.*, c.candidatos
FROM p
LEFT JOIN LATERAL (
  SELECT string_agg(e.cuit || ' ' || e.razon_social || coalesce(' (' || e.fecha_contrato_social || ')', ''), ' | '
                    ORDER BY similarity(e.nombre_norm, p.nombre_norm) DESC) AS candidatos
  FROM (SELECT * FROM ref.empresa e
        WHERE e.nombre_base = ref.norm_nombre_base(p.ejemplo) OR e.nombre_norm % p.nombre_norm
        ORDER BY similarity(e.nombre_norm, p.nombre_norm) DESC LIMIT 5) e
) c ON true;
