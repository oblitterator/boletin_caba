-- =====================================================================
-- Boletín Oficial CABA v2 — esquema base
-- Idempotente: se puede correr N veces. No toca el esquema public (v1).
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE SCHEMA IF NOT EXISTS ctl;        -- control de ejecuciones y errores
CREATE SCHEMA IF NOT EXISTS ref;        -- maestros: empresas, IPC, umbrales
CREATE SCHEMA IF NOT EXISTS bo;         -- Boletín Oficial (crudo + parseado)
CREATE SCHEMA IF NOT EXISTS bac;        -- Buenos Aires Compras (OCDS snapshots)
CREATE SCHEMA IF NOT EXISTS analitica;  -- vistas de procesos y señales

-- ---------------------------------------------------------------------
-- Normalización de razones sociales: ÚNICA fuente de verdad (SQL).
-- Python nunca normaliza nombres; siempre lee la columna generada.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ref.norm_nombre(t text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT nullif(btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    -- 3) letras sueltas consecutivas se unen: "S A C I F" -> "SACIF", "M H" -> "MH"
    regexp_replace(
      -- 2) espacios simples
      regexp_replace(
        -- 1) mayúsculas, sin acentos, & -> Y, puntuación -> espacio
        regexp_replace(translate(upper(coalesce(t,'')), 'ÁÉÍÓÚÜÑÀÈÌÒÙ&', 'AEIOUUNAEIOUY'), '[^A-Z0-9 ]', ' ', 'g'),
        '\s+', ' ', 'g'),
      '\m([A-Z]) (?=[A-Z]\M)', '\1', 'g'),
    '\mSOCIEDAD ANONIMA UNIPERSONAL\M', 'SAU', 'g'),
    '\mSOCIEDAD DE RESPONSABILIDAD LIMITADA\M', 'SRL', 'g'),
    '\mSOCIEDAD POR ACCIONES SIMPLIFICADA\M', 'SAS', 'g'),
    '\mSOCIEDAD ANONIMA\M', 'SA', 'g'),
    '\m(LA FIRMA|FIRMA)\M', '', 'g'),
    '\s+', ' ', 'g')), '')
$$;

-- Nombre sin forma societaria, para un segundo nivel de matching
CREATE OR REPLACE FUNCTION ref.norm_nombre_base(t text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT nullif(btrim(regexp_replace(regexp_replace(ref.norm_nombre(t),
    '\m(SA|SRL|SAS|SAU|SACI|SAIC|SACIF|SAICF|SAICFA|SAICYF|SACIFIA|ICISA|SH|SE|SC|SCA|SCS|UTE|LTDA|COOP)\M', '', 'g'),
    '\s+', ' ', 'g')), '')
$$;

-- ---------------------------------------------------------------------
-- CONTROL
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctl.ejecucion (
  id          bigserial PRIMARY KEY,
  comando     text NOT NULL,
  parametros  jsonb,
  inicio      timestamptz NOT NULL DEFAULT now(),
  fin         timestamptz,
  ok          boolean,
  resumen     jsonb
);

-- Errores operativos: NUNCA se guardan como dato en tablas de negocio.
CREATE TABLE IF NOT EXISTS ctl.error (
  id          bigserial PRIMARY KEY,
  ejecucion_id bigint REFERENCES ctl.ejecucion(id),
  etapa       text NOT NULL,          -- 'api_boletin' | 'pdf' | 'parser' | 'bac'
  clave       text NOT NULL,          -- fecha, id_norma, url...
  mensaje     text,
  ocurrido_en timestamptz NOT NULL DEFAULT now(),
  resuelto_en timestamptz
);
CREATE INDEX IF NOT EXISTS error_abiertos_idx ON ctl.error (etapa, clave) WHERE resuelto_en IS NULL;

-- Qué fechas ya se consultaron a la API (evita re-consultar fines de semana)
CREATE TABLE IF NOT EXISTS ctl.fecha_consultada (
  fecha        date PRIMARY KEY,
  resultado    text NOT NULL CHECK (resultado IN ('con_boletin','sin_boletin','error')),
  consultado_en timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- REFERENCIA
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.empresa (
  cuit                 bigint PRIMARY KEY,
  razon_social         text,
  nombre_norm          text GENERATED ALWAYS AS (ref.norm_nombre(razon_social)) STORED,
  nombre_base          text GENERATED ALWAYS AS (ref.norm_nombre_base(razon_social)) STORED,
  tipo_societario      text,
  fecha_contrato_social date,
  dom_fiscal           text,
  dom_fiscal_norm      text GENERATED ALWAYS AS (ref.norm_nombre(dom_fiscal)) STORED,
  dom_legal            text,
  fuente               text NOT NULL,      -- 'rns' (Registro Nac. Sociedades) | 'bac' | 'bo'
  actualizado_en       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS empresa_norm_idx  ON ref.empresa (nombre_norm);
CREATE INDEX IF NOT EXISTS empresa_base_idx  ON ref.empresa (nombre_base);
CREATE INDEX IF NOT EXISTS empresa_trgm_idx  ON ref.empresa USING gin (nombre_norm gin_trgm_ops);
CREATE INDEX IF NOT EXISTS empresa_dom_idx   ON ref.empresa (dom_fiscal_norm);

-- Alias nombre -> CUIT. Se alimenta solo (BO con CUIT en texto, BAC) y a mano.
CREATE TABLE IF NOT EXISTS ref.empresa_alias (
  nombre_norm  text NOT NULL,
  cuit         bigint NOT NULL,
  fuente       text NOT NULL CHECK (fuente IN ('bo_cuit_en_texto','bac','manual')),
  veces        int NOT NULL DEFAULT 1,
  PRIMARY KEY (nombre_norm, cuit)
);

-- Índice de precios para pasar a pesos constantes (lo carga el usuario)
CREATE TABLE IF NOT EXISTS ref.ipc (
  mes     date PRIMARY KEY,               -- primer día del mes
  indice  numeric NOT NULL
);

-- Topes por procedimiento (para detectar fraccionamiento). Los carga el usuario
-- desde la normativa vigente de cada año (unidades de compra x valor UC).
CREATE TABLE IF NOT EXISTS ref.umbral_procedimiento (
  vigente_desde date NOT NULL,
  tipo_proc     text NOT NULL,            -- 'CME' | 'CDI' | ...
  monto_tope    numeric NOT NULL,
  norma         text,
  PRIMARY KEY (vigente_desde, tipo_proc)
);

-- ---------------------------------------------------------------------
-- BOLETÍN OFICIAL
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bo.boletin (
  numero             int PRIMARY KEY,
  fecha_publicacion  date NOT NULL,
  es_separata        boolean NOT NULL DEFAULT false,
  nombre             text,
  url_boletin        text,
  raw                jsonb,
  cargado_en         timestamptz NOT NULL DEFAULT now(),
  actualizado_en     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS boletin_fecha_idx ON bo.boletin (fecha_publicacion);

CREATE TABLE IF NOT EXISTS bo.norma (
  id_norma           bigint PRIMARY KEY,
  boletin_numero     int REFERENCES bo.boletin(numero),   -- NULL en lo migrado de v1
  fecha_publicacion  date NOT NULL,
  seccion            text,          -- 'Licitaciones', 'Poder Ejecutivo', ...
  tipo               text,          -- 'Licitación Pública', 'Resolución', ...
  organismo          text,
  nombre             text,
  sumario            text,
  url_norma          text,
  raw                jsonb,
  origen             text NOT NULL DEFAULT 'api' CHECK (origen IN ('api','migracion_v1')),
  cargado_en         timestamptz NOT NULL DEFAULT now(),
  actualizado_en     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS norma_fecha_idx   ON bo.norma (fecha_publicacion);
CREATE INDEX IF NOT EXISTS norma_seccion_idx ON bo.norma (seccion, tipo);
CREATE INDEX IF NOT EXISTS norma_bol_idx     ON bo.norma (boletin_numero);

-- Texto del PDF. Estado explícito: los errores no se mezclan con el texto.
CREATE TABLE IF NOT EXISTS bo.documento (
  id_norma      bigint PRIMARY KEY REFERENCES bo.norma(id_norma),
  estado        text NOT NULL CHECK (estado IN ('pendiente','ok','vacio','error')),
  intentos      int NOT NULL DEFAULT 0,
  ultimo_error  text,
  sha256        text,
  paginas       int,
  texto         text,
  extraido_en   timestamptz,
  actualizado_en timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS documento_estado_idx ON bo.documento (estado);

-- Un acto de licitación parseado (llamado, preadjudicación, adjudicación, ...)
CREATE TABLE IF NOT EXISTS bo.acto (
  id_norma         bigint PRIMARY KEY REFERENCES bo.norma(id_norma),
  codigo_proceso   text,           -- '401-0147-LPU22' o formato viejo '5072/SIGAF/20'
  uoa              int,            -- unidad operativa de adquisiciones (prefijo)
  tipo_proc        text,           -- LPU, CME, CDI, LPR, ...
  anio_proc        int,
  tipo_licitacion  text,           -- texto del nombre: 'Licitación Pública', ...
  etapa            text,           -- llamado|preadjudicacion|adjudicacion|prorroga|circular|...|otra
  etapa_conflicto  boolean NOT NULL DEFAULT false,  -- nombre de la norma y título del texto no coinciden
  expediente       text,
  objeto           text,
  monto_total      numeric,
  monto_origen     text,           -- 'texto' | 'suma_firmas' (sumado de los montos por firma)
  moneda           text,           -- ARS | USD
  n_ofertas        int,            -- solo si el texto lo dice o lista los oferentes
  n_firmas_identificadas int NOT NULL DEFAULT 0,  -- cota inferior de oferentes
  unica_oferta     boolean,
  fracasada        boolean NOT NULL DEFAULT false,
  confianza        text NOT NULL CHECK (confianza IN ('alta','media','baja')),
  faltantes        text[] NOT NULL DEFAULT '{}',
  parser_version   text NOT NULL,
  parseado_en      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS acto_proceso_idx ON bo.acto (codigo_proceso);
CREATE INDEX IF NOT EXISTS acto_etapa_idx   ON bo.acto (etapa);

-- Firmas mencionadas en un acto, con su rol
CREATE TABLE IF NOT EXISTS bo.acto_firma (
  id_norma      bigint NOT NULL REFERENCES bo.acto(id_norma) ON DELETE CASCADE,
  orden         int NOT NULL,
  nombre_raw    text NOT NULL,
  nombre_norm   text GENERATED ALWAYS AS (ref.norm_nombre(nombre_raw)) STORED,
  rol           text NOT NULL CHECK (rol IN ('adjudicada','preadjudicada','oferente','desestimada','mencionada')),
  monto         numeric,          -- solo si el texto lo asigna a ESTA firma
  cuit_texto    bigint,           -- CUIT tal como aparece en el texto
  cuit          bigint,           -- CUIT resuelto (texto, alias o padrón)
  resolucion    text CHECK (resolucion IN ('cuit_en_texto','alias','padron_exacto','padron_base','padron_trgm','sin_resolver')),
  score         real,
  PRIMARY KEY (id_norma, orden)
);
CREATE INDEX IF NOT EXISTS acto_firma_cuit_idx ON bo.acto_firma (cuit);
CREATE INDEX IF NOT EXISTS acto_firma_norm_idx ON bo.acto_firma (nombre_norm);

-- ---------------------------------------------------------------------
-- BAC (OCDS aplanado de datos abiertos). Cada descarga se registra;
-- las filas se upsertean por ocid y se conserva cuándo se vieron.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bac.snapshot (
  id             bigserial PRIMARY KEY,
  url            text NOT NULL,
  descargado_en  timestamptz NOT NULL DEFAULT now(),
  last_modified  text,
  sha256         text NOT NULL,
  filas          int
);
CREATE UNIQUE INDEX IF NOT EXISTS snapshot_sha_idx ON bac.snapshot (sha256);

CREATE TABLE IF NOT EXISTS bac.release (
  ocid              text PRIMARY KEY,
  codigo_proceso    text,
  metodo            text,       -- open | direct
  metodo_detalle    text,       -- LICITACION PUBLICA | CONTRATACION MENOR | ...
  estado            text,
  comprador_id      text,
  comprador         text,
  titulo            text,
  fecha             timestamptz,
  monto_estimado    numeric,
  primer_snapshot   bigint REFERENCES bac.snapshot(id),
  ultimo_snapshot   bigint REFERENCES bac.snapshot(id),
  raw               jsonb
);
CREATE INDEX IF NOT EXISTS release_proceso_idx ON bac.release (codigo_proceso);

CREATE TABLE IF NOT EXISTS bac.adjudicacion (
  ocid            text NOT NULL REFERENCES bac.release(ocid) ON DELETE CASCADE,
  award_id        text NOT NULL,
  cuit            bigint,
  proveedor       text,
  monto           numeric,
  moneda          text,
  fecha           timestamptz,
  estado          text,
  PRIMARY KEY (ocid, award_id)
);
CREATE INDEX IF NOT EXISTS bac_adj_cuit_idx ON bac.adjudicacion (cuit);

-- ---------------------------------------------------------------------
-- Columnas agregadas después de la primera versión (para bases ya creadas)
-- ---------------------------------------------------------------------
ALTER TABLE bo.acto ADD COLUMN IF NOT EXISTS monto_origen text;

-- ---------------------------------------------------------------------
-- Las columnas *_norm son GENERATED: si cambia ref.norm_nombre() no se
-- recalculan solas. Al subir este número, init-db las recalcula una vez.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctl.version_normalizacion (id int PRIMARY KEY DEFAULT 1 CHECK (id = 1), version int NOT NULL);
DO $$
DECLARE v_actual int := 2;   -- subir cuando cambie ref.norm_nombre / norm_nombre_base
BEGIN
  IF coalesce((SELECT version FROM ctl.version_normalizacion), 0) < v_actual THEN
    UPDATE ref.empresa SET razon_social = razon_social, dom_fiscal = dom_fiscal;   -- fuerza el recálculo
    UPDATE bo.acto_firma SET nombre_raw = nombre_raw;
    DELETE FROM ref.empresa_alias a WHERE a.fuente <> 'manual';                   -- se re-aprenden en resolver
    INSERT INTO ctl.version_normalizacion (id, version) VALUES (1, v_actual)
      ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version;
  END IF;
END $$;
