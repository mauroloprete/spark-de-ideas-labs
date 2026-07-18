-- Setup de Lakehouse Federation en Databricks.
-- Correr en un SQL Warehouse (Pro o Serverless, canal 2023.40+).
-- Antes: guardar las credenciales de Neon en un secret scope (ver README, paso 3).

-- 1. La connection: host + credenciales. Databricks recomienda
--    pasar las credenciales con secret(), no como texto plano.
--    Reemplazá <TU_HOST_NEON> por el host de tu proyecto
--    (ej.: ep-xxxx-yyyy-pooler.c-2.us-east-1.aws.neon.tech)
CREATE CONNECTION pg_operacional TYPE postgresql
OPTIONS (
  host '<TU_HOST_NEON>',
  port '5432',
  user secret('lab-federation', 'pg-user'),
  password secret('lab-federation', 'pg-password')
);

-- 2. El foreign catalog: espeja UNA database del servidor.
--    'neondb' es la database default de un proyecto Neon.
CREATE FOREIGN CATALOG pg_ventas
USING CONNECTION pg_operacional
OPTIONS (database 'neondb');

-- 3. (Opcional) Permisos finos, como en cualquier catálogo de UC.
-- GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG pg_ventas TO `data-analysts`;

-- Sanity check: lectura en vivo contra Neon
SELECT count(*) FROM pg_ventas.public.orders;
