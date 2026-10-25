-- 01 — El dataset del lab (correr en cualquier warehouse)

-- 20M de pedidos, 200k clientes, tablas MANAGED de UC (requisito de la Beta).
SELECT 'pedidos' AS tabla, count(*) AS filas FROM dev_bronze.lakehouse_rt_tips18.pedidos
UNION ALL
SELECT 'clientes', count(*) FROM dev_bronze.lakehouse_rt_tips18.clientes;

-- Liquid Clustering por (cliente_id, ts): el patrón de acceso del serving.
-- Salida esperada: clusteringColumns = [["cliente_id"],["ts"]]
DESCRIBE DETAIL dev_bronze.lakehouse_rt_tips18.pedidos;
