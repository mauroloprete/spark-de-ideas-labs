-- Lab Tips #16 — Data Quality
-- Catálogos y schemas. Si tu metastore no tiene storage root default,
-- agregá MANAGED LOCATION '<url de tu external location>' a los CREATE CATALOG.
CREATE CATALOG IF NOT EXISTS lab;
CREATE SCHEMA IF NOT EXISTS lab.bronze;
CREATE SCHEMA IF NOT EXISTS lab.dq;
CREATE CATALOG IF NOT EXISTS gobernanza;
CREATE SCHEMA IF NOT EXISTS gobernanza.calidad;

-- Las reglas como datos: una fila por regla, agrupadas por etiqueta
CREATE OR REPLACE TABLE gobernanza.calidad.reglas (
  nombre    STRING,   -- identificador de la regla
  condicion STRING,   -- el constraint SQL booleano
  etiqueta  STRING    -- agrupa reglas por criterio o por tabla
);

INSERT INTO gobernanza.calidad.reglas VALUES
  ('monto_positivo',   'monto > 0',                 'validez'),
  ('cliente_presente', 'cliente_id IS NOT NULL',    'validez'),
  ('moneda_conocida',  "moneda IN ('UYU', 'USD')",  'validez'),
  ('fecha_no_futura',  'fecha <= current_date()',   'plausibilidad');

-- Bronze con datos sucios a propósito:
-- 5.000 clientes nulos (id % 20), 4.000 montos <= 0 (id % 25),
-- 2.000 monedas inválidas (id % 50)
CREATE OR REPLACE TABLE lab.bronze.transacciones AS
SELECT
  id AS transaccion_id,
  CASE WHEN id % 20 = 0 THEN NULL
       ELSE concat('cliente_', id % 500) END      AS cliente_id,
  CASE WHEN id % 25 = 0 THEN -1 * (id % 900)
       ELSE (id % 900) + 10 END                   AS monto,
  CASE WHEN id % 50 = 0 THEN 'XXX'
       ELSE element_at(array('UYU', 'USD'), CAST(1 + id % 2 AS INT)) END AS moneda,
  date_add(DATE '2026-07-01', CAST(id % 30 AS INT)) AS fecha
FROM range(100000) AS t(id);

-- Verificación del seed: 100000 | 4000 | 5000 | 2000
SELECT
  count(*)                        AS total,
  count_if(monto <= 0)            AS monto_malo,
  count_if(cliente_id IS NULL)    AS cliente_nulo,
  count_if(moneda = 'XXX')        AS moneda_mala
FROM lab.bronze.transacciones;
