-- Seed del lab: 5 millones de órdenes sintéticas (~300 MB, entra en el free tier de Neon).
-- Correr en el SQL Editor de Neon o con: psql "<connection-string>" -f src/seed_neon.sql

CREATE TABLE orders AS
SELECT
  g AS order_id,
  'cliente_' || (g % 1000) AS cliente,
  (ARRAY['web','app','tienda'])[1 + g % 3] AS canal,
  DATE '2025-01-01' + (g % 540) AS fecha,
  round((random() * 900 + 100)::numeric, 2) AS monto
FROM generate_series(1, 5000000) AS g;

-- Verificación
SELECT count(*), min(fecha), max(fecha),
       pg_size_pretty(pg_total_relation_size('orders'))
FROM orders;
