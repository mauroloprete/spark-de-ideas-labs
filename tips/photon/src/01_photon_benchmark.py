# Databricks notebook source
# MAGIC %md
# MAGIC # Lab — Databricks Tips #12: Photon
# MAGIC
# MAGIC Benchmark con/sin Photon: mismo código, mismo node type, dos runtimes.
# MAGIC
# MAGIC - **Datos**: 200M filas sintéticas estilo TPC-DS (`<catalog>.<schema>.store_sales`)
# MAGIC - **Query**: aggregation pesada + window function (territorio Photon)
# MAGIC - **Medición**: 5 corridas, mediana de wall-clock, sink `noop`
# MAGIC
# MAGIC Este lab necesita **classic compute** (para poder apagar Photon) — no corre en
# MAGIC Free Edition, que es serverless-only y siempre tiene Photon activo.
# MAGIC
# MAGIC Post: https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-12-photon/

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_bronze")
dbutils.widgets.text("schema", "photon_bench")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
tabla = f"{catalog}.{schema}.store_sales"

# COMMAND ----------

# MAGIC %md ## 0. Contexto del run

# COMMAND ----------

import json

spark_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "?")

ctx = {
    "spark_version": spark_version,
    "photon": "photon" in spark_version.lower(),
    "node_type": spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "?"),
    "workers": spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers", "?"),
}
print(json.dumps(ctx, indent=2))

# COMMAND ----------

# MAGIC %md ## 1. Generar datos (idempotente — solo si no existe)

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

if not spark.catalog.tableExists(tabla):
    (
        spark.range(200_000_000)
        .withColumn("store_id", (F.col("id") % 500).cast("int"))
        .withColumn("item_id", (F.col("id") % 100_000).cast("int"))
        .withColumn("fecha", F.date_add(F.lit("2025-01-01"), (F.col("id") % 540).cast("int")))
        .withColumn("cantidad", (F.col("id") % 10 + 1).cast("int"))
        .withColumn("precio", F.round(F.rand(seed=42) * 500, 2))
        .write.mode("overwrite")
        .saveAsTable(tabla)
    )
    print("Tabla creada")
else:
    print("Tabla ya existe — se reutiliza")

print(f"filas: {spark.table(tabla).count():,}")

# COMMAND ----------

# MAGIC %md ## 2. La query del benchmark (+ plan de ejecución)

# COMMAND ----------

query = f"""
    WITH ventas_diarias AS (
        SELECT store_id, fecha,
               SUM(cantidad * precio) AS revenue,
               COUNT(DISTINCT item_id)  AS items_distintos
        FROM {tabla}
        WHERE fecha >= '2025-06-01'
        GROUP BY store_id, fecha
    )
    SELECT store_id, fecha, revenue,
           AVG(revenue) OVER (
               PARTITION BY store_id ORDER BY fecha
               ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
           ) AS revenue_7d
    FROM ventas_diarias
    ORDER BY store_id, fecha
"""

plan = spark.sql(query)._jdf.queryExecution().executedPlan().toString()
print(plan[:3000])

# COMMAND ----------

# MAGIC %md ## 3. Benchmark — 5 corridas, mediana

# COMMAND ----------

import time

runs = []
for i in range(5):
    spark.sql("CLEAR CACHE")
    t0 = time.perf_counter()
    spark.sql(query).write.mode("overwrite").format("noop").save()
    elapsed = time.perf_counter() - t0
    runs.append(elapsed)
    print(f"run {i + 1}: {elapsed:.1f}s")

resultado = {
    **ctx,
    "runs_s": [round(r, 1) for r in runs],
    "mediana_s": round(sorted(runs)[2], 1),
    "plan_snippet": plan[:1500],
}
print(json.dumps({k: v for k, v in resultado.items() if k != "plan_snippet"}, indent=2))

# COMMAND ----------

# MAGIC %md ## 4. Resultado para recolectar desde el job run

# COMMAND ----------

dbutils.notebook.exit(json.dumps(resultado))
