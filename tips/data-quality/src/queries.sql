-- Lab Tips #16 — Data Quality
-- Correr contra un SQL Warehouse después de que el pipeline completó su update.

-- 1. Métricas de expectations por regla, desde el event log
--    (reemplazá <pipeline_id> por el ID de tu pipeline)
SELECT
  row_exp.dataset AS dataset,
  row_exp.name AS expectation,
  SUM(row_exp.passed_records) AS pasan,
  SUM(row_exp.failed_records) AS fallan
FROM (
  SELECT explode(
    from_json(
      details:flow_progress:data_quality:expectations,
      'array<struct<name:string, dataset:string, passed_records:int, failed_records:int>>'
    )
  ) AS row_exp
  FROM event_log('<pipeline_id>')
  WHERE event_type = 'flow_progress'
)
GROUP BY row_exp.dataset, row_exp.name
ORDER BY fallan DESC;

-- 2. El contrato del patrón: silver + cuarentena = bronze, exacto
SELECT
  (SELECT count(*) FROM lab.bronze.transacciones)        AS bronze,
  (SELECT count(*) FROM lab.dq.silver_transacciones)     AS silver,
  (SELECT count(*) FROM lab.dq.cuarentena_transacciones) AS cuarentena;
