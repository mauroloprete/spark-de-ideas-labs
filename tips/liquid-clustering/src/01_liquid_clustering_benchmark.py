# Databricks notebook source
# MAGIC %md
# MAGIC # Lab — Databricks Tips #14: Liquid Clustering
# MAGIC
# MAGIC Compara tres layouts sobre los MISMOS datos y mide cuánto poda cada uno:
# MAGIC
# MAGIC - **Particionado por fecha** (`PARTITIONED BY`)
# MAGIC - **Z-ORDER** (`OPTIMIZE ... ZORDER BY`)
# MAGIC - **Liquid Clustering** (`CLUSTER BY`)
# MAGIC
# MAGIC La query filtra por `cliente` **y** `fecha`. Corre en **Free Edition** (serverless, Photon siempre activo).
# MAGIC
# MAGIC Post: https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-14-liquid-clustering/

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "liquid_clustering_lab")
dbutils.widgets.text("rows", "200000000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
rows = int(dbutils.widgets.get("rows"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

# Query única del benchmark: filtra por cliente Y fecha
QUERY = ("SELECT count(*) FROM {t} "
         "WHERE cliente = 4242 AND fecha BETWEEN '2024-06-01' AND '2024-06-30'")

# COMMAND ----------

# MAGIC %md ## 1. Dataset sintético (idempotente)
# MAGIC
# MAGIC `cliente` es de alta cardinalidad (50.000 valores). Es la columna donde el particionado por fecha no puede podar.

# COMMAND ----------

from pyspark.sql import functions as F

if not spark.catalog.tableExists("ventas_base"):
    (spark.range(0, rows)
        .withColumn("cliente", (F.rand(seed=1) * 50000).cast("int"))
        .withColumn("fecha", F.expr("date_add('2024-01-01', cast(rand(2)*600 as int))"))
        .withColumn("monto", (F.rand(seed=3) * 1000))
        .write.mode("overwrite").saveAsTable("ventas_base"))
    print("ventas_base creada")
else:
    print("ventas_base ya existe — se reutiliza")

# COMMAND ----------

# MAGIC %md ## 2. Los tres layouts sobre los mismos datos

# COMMAND ----------

# (a) Particionado por fecha
spark.sql("DROP TABLE IF EXISTS ventas_part")
spark.sql("CREATE TABLE ventas_part PARTITIONED BY (fecha) AS SELECT * FROM ventas_base")

# (b) Z-ORDER por cliente, fecha
spark.sql("DROP TABLE IF EXISTS ventas_zorder")
spark.sql("CREATE TABLE ventas_zorder AS SELECT * FROM ventas_base")
spark.sql("OPTIMIZE ventas_zorder ZORDER BY (cliente, fecha)")

# (c) Liquid Clustering por cliente, fecha
spark.sql("DROP TABLE IF EXISTS ventas_liquid")
spark.sql("CREATE TABLE ventas_liquid CLUSTER BY (cliente, fecha) AS SELECT * FROM ventas_base")
spark.sql("OPTIMIZE ventas_liquid FULL")   # recluster total la primera vez

print("Tablas listas")

# COMMAND ----------

# MAGIC %md ## 3. Total de archivos y tamaño (esto SÍ es programático)
# MAGIC
# MAGIC `DESCRIBE DETAIL` funciona en cualquier compute y muestra el efecto del layout: cuántos archivos generó cada estrategia.

# COMMAND ----------

import time, json

def detail(t):
    r = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles", "sizeInBytes").collect()[0]
    return int(r["numFiles"]), int(r["sizeInBytes"])

def wallclock(t, n=3):
    times = []
    for _ in range(n):
        t0 = time.time()
        spark.sql(QUERY.format(t=t)).collect()
        times.append(time.time() - t0)
    return round(min(times), 2)

results = {}
for key, t in [("particionado", "ventas_part"), ("zorder", "ventas_zorder"), ("liquid", "ventas_liquid")]:
    nf, sz = detail(t)
    results[key] = {"tabla": t, "archivos_totales": nf,
                    "size_mb": round(sz / 1024 / 1024, 1),
                    "wallclock_s_min": wallclock(t)}

print(json.dumps(results, indent=2))

# COMMAND ----------

# MAGIC %md ## 4. Archivos leídos vs podados (la métrica estrella)
# MAGIC
# MAGIC En **serverless** el motor usa Spark Connect: no se puede leer el plan de ejecución por JVM.
# MAGIC Para sacar *files pruned* / *files read* de forma reproducible hay dos caminos, los dos oficiales:
# MAGIC
# MAGIC **A) Query profile (UI)** — corré la query y abrí el query profile: los campos **Files pruned** y **Files read** están ahí. Es lo más directo y sirve para un screenshot.
# MAGIC
# MAGIC **B) Query history (programático)** — corré las tres queries en un **SQL Warehouse** (ver `src/queries.sql`) y leé las métricas del history. Es lo que usa el post. Desde la CLI de Databricks:
# MAGIC
# MAGIC ```bash
# MAGIC databricks api get /api/2.0/sql/history/queries \
# MAGIC   --json '{"include_metrics": true, "max_results": 20}' \
# MAGIC   | jq -r '.res[]
# MAGIC       | select(.query_text | test("ventas_(part|zorder|liquid)"))
# MAGIC       | "\(.query_text | capture("ventas_(?<t>[a-z]+)").t)
# MAGIC          read_files=\(.metrics.read_files_count)
# MAGIC          pruned_files=\(.metrics.pruned_files_count)
# MAGIC          read_bytes=\(.metrics.read_bytes)"'
# MAGIC ```
# MAGIC
# MAGIC También podés leerlo con SQL, si tenés habilitado el system schema `query`:

# COMMAND ----------

# Best-effort: pruning desde el system table (puede tener latencia de ingesta y requiere el schema `query` habilitado)
try:
    spark.sql(f"""
        SELECT
            regexp_extract(statement_text, 'ventas_(part|zorder|liquid)', 0) AS tabla,
            read_files, pruned_files, read_bytes, total_duration_ms
        FROM system.query.history
        WHERE statement_text LIKE '%{schema}.ventas\\_%'
          AND statement_text LIKE '%cliente = 4242%'
        ORDER BY start_time DESC
        LIMIT 10
    """).show(truncate=False)
except Exception as e:
    print("system.query.history no disponible (usá el query profile o la history API):", str(e)[:200])

# COMMAND ----------

dbutils.notebook.exit(json.dumps(results))
