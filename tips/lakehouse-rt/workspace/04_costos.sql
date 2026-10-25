-- 04 — Cuánto costó el lab (correr en un warehouse PRO: system tables
-- no están soportadas en RT). Los registros de billing demoran horas: correr
-- al día siguiente del benchmark.

-- El RT factura en su propio SKU. La promo hasta el 31/1/2027 NO es un
-- descuento de precio: es una tasa de emisión de DBUs 30% menor
-- (10 DBUs consumidas se facturan como 7).
SELECT
    usage_date,
    sku_name,
    round(sum(usage_quantity), 2)                       AS dbus,
    any_value(usage_metadata.warehouse_id)              AS warehouse_id
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
  AND (sku_name LIKE '%LAKEHOUSE_SERVERLESS%'           -- Lakehouse//RT
       OR sku_name LIKE '%SERVERLESS_SQL%')             -- warehouse PRO serverless
GROUP BY usage_date, sku_name
ORDER BY usage_date DESC, sku_name;
