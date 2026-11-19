-- =====================================================================
-- Funciones de mantenimiento (se definen en init-db; se ejecutan desde la CLI)
-- =====================================================================

-- ---------------------------------------------------------------------
-- Migración desde la v1 (esquema public). Reutiliza los textos ya bajados
-- para no volver a descargar 2019–2024. Idempotente: se puede correr N veces.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ctl.migrar_v1() RETURNS TABLE (paso text, filas bigint)
LANGUAGE plpgsql AS $$
DECLARE v_n bigint;
BEGIN
  IF to_regclass('public.normas') IS NOT NULL THEN
    INSERT INTO bo.norma (id_norma, fecha_publicacion, seccion, tipo, organismo, nombre, sumario, url_norma, raw, origen)
    SELECT n.id_norma, n.fecha_publicacion, n.categoria, n.tipo, coalesce(n.desc_emisor, n.emisor),
           n.nombre, n.sumario, n.url_norma, to_jsonb(n) - 'anexos', 'migracion_v1'
    FROM public.normas n
    WHERE n.fecha_publicacion IS NOT NULL
    ON CONFLICT (id_norma) DO NOTHING;           -- lo bajado por API (v2) tiene prioridad
    GET DIAGNOSTICS v_n = ROW_COUNT; paso := 'normas'; filas := v_n; RETURN NEXT;
  END IF;

  IF to_regclass('public.licitaciones') IS NOT NULL THEN
    INSERT INTO bo.documento (id_norma, estado, intentos, texto, extraido_en)
    SELECT l.id_norma,
           CASE WHEN coalesce(btrim(l.texto),'') = '' THEN 'pendiente' ELSE 'ok' END,
           0, nullif(l.texto, ''), now()
    FROM public.licitaciones l
    JOIN bo.norma n ON n.id_norma = l.id_norma
    WHERE l.estado IS DISTINCT FROM 'Timeout' AND l.estado NOT ILIKE 'Error%'
    ON CONFLICT (id_norma) DO NOTHING;
    GET DIAGNOSTICS v_n = ROW_COUNT; paso := 'documentos_ok'; filas := v_n; RETURN NEXT;

    -- los que en v1 fallaron (Timeout/Error) quedan pendientes para re-descargar
    INSERT INTO bo.documento (id_norma, estado)
    SELECT l.id_norma, 'pendiente' FROM public.licitaciones l
    JOIN bo.norma n ON n.id_norma = l.id_norma
    WHERE l.estado = 'Timeout' OR l.estado ILIKE 'Error%'
    ON CONFLICT (id_norma) DO NOTHING;
    GET DIAGNOSTICS v_n = ROW_COUNT; paso := 'documentos_a_reintentar'; filas := v_n; RETURN NEXT;
  END IF;

  -- normas de Licitaciones sin texto en v1 -> pendientes
  INSERT INTO bo.documento (id_norma, estado)
  SELECT id_norma, 'pendiente' FROM bo.norma WHERE seccion = 'Licitaciones'
  ON CONFLICT (id_norma) DO NOTHING;
  GET DIAGNOSTICS v_n = ROW_COUNT; paso := 'licitaciones_sin_texto'; filas := v_n; RETURN NEXT;

  IF to_regclass('public.registro_empresas') IS NOT NULL THEN
    INSERT INTO ref.empresa (cuit, razon_social, tipo_societario, fecha_contrato_social, dom_fiscal, dom_legal, fuente)
    SELECT e.cuit, e.razon_social, e.tipo_societario, e.fecha_hora_contrato_social::date,
           nullif(concat_ws(' ', e.dom_fiscal_calle, e.dom_fiscal_numero, e.dom_fiscal_piso, e.dom_fiscal_departamento,
                            e.dom_fiscal_localidad, e.dom_fiscal_provincia), ''),
           nullif(concat_ws(' ', e.dom_legal_calle, e.dom_legal_numero, e.dom_legal_piso, e.dom_legal_departamento,
                            e.dom_legal_localidad, e.dom_legal_provincia), ''),
           'rns'
    FROM public.registro_empresas e
    ON CONFLICT (cuit) DO UPDATE SET
      razon_social = EXCLUDED.razon_social, tipo_societario = EXCLUDED.tipo_societario,
      fecha_contrato_social = EXCLUDED.fecha_contrato_social, dom_fiscal = EXCLUDED.dom_fiscal,
      dom_legal = EXCLUDED.dom_legal, fuente = 'rns', actualizado_en = now();
    GET DIAGNOSTICS v_n = ROW_COUNT; paso := 'empresas_rns'; filas := v_n; RETURN NEXT;
  END IF;
END $$;


-- ---------------------------------------------------------------------
-- Resolución nombre -> CUIT de las firmas de los actos.
-- Orden de confianza: CUIT en el texto > alias manual > alias aprendido (BO con CUIT, BAC)
--                     > padrón exacto > padrón sin forma societaria > trigramas (>= umbral)
-- Solo asigna si el candidato es ÚNICO; si hay ambigüedad queda 'sin_resolver'.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ref.resolver_cuit(umbral_trgm real DEFAULT 0.85)
RETURNS TABLE (resolucion text, firmas bigint)
LANGUAGE plpgsql AS $$
BEGIN
  -- 1) aprender alias de los textos que traen CUIT y de BAC
  INSERT INTO ref.empresa_alias (nombre_norm, cuit, fuente, veces)
  SELECT af.nombre_norm, af.cuit_texto, 'bo_cuit_en_texto', count(*)
  FROM bo.acto_firma af WHERE af.cuit_texto IS NOT NULL AND af.nombre_norm IS NOT NULL
  GROUP BY 1, 2
  ON CONFLICT (nombre_norm, cuit) DO UPDATE SET veces = EXCLUDED.veces
    WHERE ref.empresa_alias.fuente <> 'manual';

  INSERT INTO ref.empresa_alias (nombre_norm, cuit, fuente, veces)
  SELECT ref.norm_nombre(a.proveedor), a.cuit, 'bac', count(*)
  FROM bac.adjudicacion a WHERE a.cuit IS NOT NULL AND ref.norm_nombre(a.proveedor) IS NOT NULL
  GROUP BY 1, 2
  ON CONFLICT (nombre_norm, cuit) DO NOTHING;

  -- empresas vistas con CUIT pero ausentes del padrón (p.ej. personas humanas)
  INSERT INTO ref.empresa (cuit, razon_social, fuente)
  SELECT DISTINCT ON (cuit_texto) cuit_texto, nombre_raw, 'bo' FROM bo.acto_firma WHERE cuit_texto IS NOT NULL
  ORDER BY cuit_texto, length(nombre_raw) DESC
  ON CONFLICT (cuit) DO NOTHING;
  INSERT INTO ref.empresa (cuit, razon_social, fuente)
  SELECT DISTINCT ON (cuit) cuit, proveedor, 'bac' FROM bac.adjudicacion WHERE cuit IS NOT NULL
  ORDER BY cuit, fecha DESC NULLS LAST
  ON CONFLICT (cuit) DO NOTHING;

  -- 2) reset (se recalcula todo: la resolución depende de alias y padrón, que cambian)
  UPDATE bo.acto_firma SET cuit = NULL, resolucion = NULL, score = NULL;

  UPDATE bo.acto_firma SET cuit = cuit_texto, resolucion = 'cuit_en_texto', score = 1
  WHERE cuit_texto IS NOT NULL;

  -- 3) alias (manual primero), solo si el nombre apunta a un único CUIT
  WITH unicos AS (
    SELECT nombre_norm, min(cuit) AS cuit, bool_or(fuente = 'manual') AS manual
    FROM ref.empresa_alias
    GROUP BY nombre_norm
    HAVING count(DISTINCT cuit) = 1 OR bool_or(fuente = 'manual')
  ), manual AS (
    SELECT nombre_norm, min(cuit) AS cuit FROM ref.empresa_alias WHERE fuente = 'manual'
    GROUP BY nombre_norm HAVING count(DISTINCT cuit) = 1
  )
  UPDATE bo.acto_firma af SET cuit = coalesce(m.cuit, u.cuit), resolucion = 'alias', score = 1
  FROM unicos u LEFT JOIN manual m USING (nombre_norm)
  WHERE af.cuit IS NULL AND af.nombre_norm = u.nombre_norm AND (m.cuit IS NOT NULL OR NOT u.manual);

  -- 4) padrón: nombre normalizado exacto y único
  WITH u AS (
    SELECT nombre_norm, min(cuit) AS cuit FROM ref.empresa
    WHERE nombre_norm IN (SELECT nombre_norm FROM bo.acto_firma WHERE cuit IS NULL)
    GROUP BY nombre_norm HAVING count(*) = 1
  )
  UPDATE bo.acto_firma af SET cuit = u.cuit, resolucion = 'padron_exacto', score = 1
  FROM u WHERE af.cuit IS NULL AND af.nombre_norm = u.nombre_norm;

  -- 5) padrón sin forma societaria (solo nombres de 6+ caracteres, para evitar genéricos)
  WITH u AS (
    SELECT nombre_base, min(cuit) AS cuit FROM ref.empresa
    WHERE length(nombre_base) >= 6
      AND nombre_base IN (SELECT ref.norm_nombre_base(nombre_raw) FROM bo.acto_firma WHERE cuit IS NULL)
    GROUP BY nombre_base HAVING count(*) = 1
  )
  UPDATE bo.acto_firma af SET cuit = u.cuit, resolucion = 'padron_base', score = 0.95
  FROM u WHERE af.cuit IS NULL AND ref.norm_nombre_base(af.nombre_raw) = u.nombre_base;

  -- 6) trigramas: mejor candidato >= umbral y claramente mejor que el segundo
  PERFORM set_config('pg_trgm.similarity_threshold', umbral_trgm::text, true);
  WITH pend AS (
    SELECT DISTINCT nombre_norm FROM bo.acto_firma
    WHERE cuit IS NULL AND length(nombre_norm) >= 6
  ), cand AS (
    SELECT p.nombre_norm, c.cuit, c.sim, c.rk, c.sim2
    FROM pend p
    CROSS JOIN LATERAL (
      SELECT e.cuit, similarity(e.nombre_norm, p.nombre_norm) AS sim,
             row_number() OVER (ORDER BY similarity(e.nombre_norm, p.nombre_norm) DESC) AS rk,
             lead(similarity(e.nombre_norm, p.nombre_norm)) OVER (ORDER BY similarity(e.nombre_norm, p.nombre_norm) DESC) AS sim2
      FROM ref.empresa e WHERE e.nombre_norm % p.nombre_norm
      ORDER BY sim DESC LIMIT 2
    ) c
  )
  UPDATE bo.acto_firma af SET cuit = c.cuit, resolucion = 'padron_trgm', score = c.sim
  FROM cand c
  WHERE af.cuit IS NULL AND af.nombre_norm = c.nombre_norm AND c.rk = 1
    AND (c.sim2 IS NULL OR c.sim - c.sim2 >= 0.05);

  UPDATE bo.acto_firma SET resolucion = 'sin_resolver' WHERE cuit IS NULL;

  RETURN QUERY SELECT af.resolucion, count(*) FROM bo.acto_firma af GROUP BY 1 ORDER BY 2 DESC;
END $$;
