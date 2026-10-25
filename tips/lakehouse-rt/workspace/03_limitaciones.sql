-- 03 — Lo que la Beta NO hace (correr SOLO conectado al warehouse RT)
-- ⚠️ El ERROR es el output buscado: la Beta es solo-SELECT (ANSI).
-- ⚠️ Conectado a un warehouse PRO estos statements SÍ corren: verificar el
--    warehouse seleccionado antes de ejecutar. El INSERT apunta a la tabla
--    de sacrificio demo_escritura por si pasa.

-- 1) DML: sin INSERT/UPDATE/DELETE/MERGE
INSERT INTO dev_bronze.lakehouse_rt_tips18.demo_escritura VALUES (2, 'no va a entrar');

-- 2) DDL: sin CREATE/ALTER/DROP
CREATE TABLE dev_bronze.lakehouse_rt_tips18.tabla_nueva (id INT);

-- 3) Permisos: sin GRANT/REVOKE
GRANT SELECT ON TABLE dev_bronze.lakehouse_rt_tips18.pedidos TO `account users`;

-- 4) Mantenimiento Delta: sin OPTIMIZE/VACUUM/ANALYZE
OPTIMIZE dev_bronze.lakehouse_rt_tips18.pedidos;

-- 5) AI Functions: tampoco están en la Beta
SELECT ai_classify('probando desde RT', array('a', 'b'));
