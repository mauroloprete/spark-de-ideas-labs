-- Benchmark de Liquid Clustering — Databricks Tips #14
-- Corré estas tres queries en un SQL Warehouse (serverless con Photon).
-- Después abrí el query profile de cada una, o leé las métricas del query history,
-- para ver "Files read" y "Files pruned". Ajustá <catalog>.<schema> a tu lab.

-- (a) Particionado por fecha: poda por fecha, pero lee todos los archivos del rango
--     porque no puede podar por cliente (alta cardinalidad).
SELECT count(*) FROM <catalog>.<schema>.ventas_part
WHERE cliente = 4242 AND fecha BETWEEN '2024-06-01' AND '2024-06-30';

-- (b) Z-ORDER (cliente, fecha): poda por las dos dimensiones.
SELECT count(*) FROM <catalog>.<schema>.ventas_zorder
WHERE cliente = 4242 AND fecha BETWEEN '2024-06-01' AND '2024-06-30';

-- (c) Liquid Clustering (cliente, fecha): poda por las dos y empaqueta con curva de Hilbert.
SELECT count(*) FROM <catalog>.<schema>.ventas_liquid
WHERE cliente = 4242 AND fecha BETWEEN '2024-06-01' AND '2024-06-30';
