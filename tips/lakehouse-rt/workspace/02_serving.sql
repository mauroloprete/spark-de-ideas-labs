-- 02 — Las 3 queries del benchmark, con parámetros fijos (versión editor
-- de las que bench.py lanza por API). Para ver la diferencia con y sin
-- result cache: correr Q1-Q3, repetir Q1 tal cual (responde del cache) y
-- después correr las variantes B. En el serverless el cache remoto vive a
-- nivel workspace y `SET use_cached_result = false` puede no cortarlo en el
-- editor, así que las variantes esquivan el cache cambiando literales.

-- Q1: estado de un cliente (lookup puntual; Liquid Clustering hace el pruning)
SELECT count(*) AS pedidos, round(sum(monto), 2) AS total, max(ts) AS ultimo_pedido
FROM dev_bronze.lakehouse_rt_tips18.pedidos
WHERE cliente_id = 123456;

-- Q2: top 10 productos de una categoría, últimas 24 h (panel de dashboard)
SELECT producto, count(*) AS n, round(sum(monto), 2) AS total
FROM dev_bronze.lakehouse_rt_tips18.pedidos
WHERE categoria = 'electro'
  AND ts >= timestampadd(HOUR, -24, (SELECT max(ts) FROM dev_bronze.lakehouse_rt_tips18.pedidos))
GROUP BY producto
ORDER BY total DESC
LIMIT 10;

-- Q3: panel por segmento de un país, últimos 7 días (join + agregación)
SELECT c.segmento, count(*) AS n, round(sum(p.monto), 2) AS total
FROM dev_bronze.lakehouse_rt_tips18.pedidos p
JOIN dev_bronze.lakehouse_rt_tips18.clientes c ON p.cliente_id = c.cliente_id
WHERE c.pais = 'UY'
  AND p.ts >= timestampadd(DAY, -7, (SELECT max(ts) FROM dev_bronze.lakehouse_rt_tips18.pedidos))
GROUP BY c.segmento
ORDER BY total DESC;

-- ───────────────────────────────────────────────────────────────────────────
-- Variantes B: mismas queries con literales nuevos, el cache no aplica.
-- Cualquier cliente_id de 1 a 200000 sirve.

-- Q1b: otro cliente
SELECT count(*) AS pedidos, round(sum(monto), 2) AS total, max(ts) AS ultimo_pedido
FROM dev_bronze.lakehouse_rt_tips18.pedidos
WHERE cliente_id = 87654;

-- Q2b: ventana de 23 h (literal nuevo, misma historia)
SELECT producto, count(*) AS n, round(sum(monto), 2) AS total
FROM dev_bronze.lakehouse_rt_tips18.pedidos
WHERE categoria = 'electro'
  AND ts >= timestampadd(HOUR, -23, (SELECT max(ts) FROM dev_bronze.lakehouse_rt_tips18.pedidos))
GROUP BY producto
ORDER BY total DESC
LIMIT 10;

-- Q3b: últimos 6 días
SELECT c.segmento, count(*) AS n, round(sum(p.monto), 2) AS total
FROM dev_bronze.lakehouse_rt_tips18.pedidos p
JOIN dev_bronze.lakehouse_rt_tips18.clientes c ON p.cliente_id = c.cliente_id
WHERE c.pais = 'UY'
  AND p.ts >= timestampadd(DAY, -6, (SELECT max(ts) FROM dev_bronze.lakehouse_rt_tips18.pedidos))
GROUP BY c.segmento
ORDER BY total DESC;

-- ───────────────────────────────────────────────────────────────────────────
-- Variantes C: otra tanda de literales nuevos (el cache remoto es por
-- workspace, no por warehouse: para comparar motores van queries frescas).

-- Q1c
SELECT count(*) AS pedidos, round(sum(monto), 2) AS total, max(ts) AS ultimo_pedido
FROM dev_bronze.lakehouse_rt_tips18.pedidos
WHERE cliente_id = 24680;

-- Q2c: ventana de 22 h
SELECT producto, count(*) AS n, round(sum(monto), 2) AS total
FROM dev_bronze.lakehouse_rt_tips18.pedidos
WHERE categoria = 'electro'
  AND ts >= timestampadd(HOUR, -22, (SELECT max(ts) FROM dev_bronze.lakehouse_rt_tips18.pedidos))
GROUP BY producto
ORDER BY total DESC
LIMIT 10;

-- Q3c: últimos 8 días
SELECT c.segmento, count(*) AS n, round(sum(p.monto), 2) AS total
FROM dev_bronze.lakehouse_rt_tips18.pedidos p
JOIN dev_bronze.lakehouse_rt_tips18.clientes c ON p.cliente_id = c.cliente_id
WHERE c.pais = 'UY'
  AND p.ts >= timestampadd(DAY, -8, (SELECT max(ts) FROM dev_bronze.lakehouse_rt_tips18.pedidos))
GROUP BY c.segmento
ORDER BY total DESC;
