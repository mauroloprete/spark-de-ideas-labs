# Databricks notebook source
# MAGIC %md
# MAGIC # Spark: pensar en distribuido
# MAGIC
# MAGIC Notebook del episodio **"Si arrancara de cero"** del podcast [Spark de Ideas](https://open.spotify.com/show/20WqZma0iLDYbg9wAEfv0w).
# MAGIC
# MAGIC Acá vemos la diferencia entre funciones nativas de Spark, wrapper functions y UDFs,
# MAGIC y por qué importa para el rendimiento.
# MAGIC
# MAGIC > Ejecutable en Databricks Community Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: crear datos de ejemplo

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import random

random.seed(42)

# Dataset de transacciones (2M filas — necesitamos volumen para que el serde duela)
rows = [
    (i, random.randint(1, 1000), float(random.randint(1, 5000)),
     random.choice(["completed", "pending", "failed"]),
     f"2026-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
     random.choice(["retail", "wholesale", "online", "partner"]))
    for i in range(2_000_000)
]

df = spark.createDataFrame(rows, ["transaction_id", "customer_id", "amount", "status", "transaction_date", "channel"])
print(f"Filas: {df.count()}")
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Funciones nativas de Spark
# MAGIC
# MAGIC Spark las entiende, las optimiza con Catalyst, y las ejecuta en la JVM directamente.

# COMMAND ----------

# Funciones nativas — Spark las optimiza
result_native = (
    df
    .filter(F.col("status") == "completed")
    .withColumn(
        "tier",
        F.when(F.col("amount") > 3000, "premium")
         .when(F.col("amount") > 1000, "standard")
         .otherwise("basic")
    )
    .withColumn(
        "risk_score",
        F.when(
            (F.col("amount") > 3000) & (F.col("channel") == "online"), 0.9
        ).when(
            F.col("amount") > 1000, 0.5
        ).otherwise(0.1)
    )
    .withColumn("channel_norm", F.upper(F.regexp_replace(F.trim(F.col("channel")), r"[^a-zA-Z]", "")))
    .withColumn("row_key", F.concat_ws("_",
        F.col("customer_id"),
        F.regexp_replace(F.col("transaction_date"), "-", ""),
        F.upper(F.substring(F.col("channel"), 1, 3))
    ))
)

result_native.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Wrapper functions ≠ UDFs
# MAGIC
# MAGIC Una función Python que usa `F.col`, `F.when`, `.withColumn` **NO es un UDF**.
# MAGIC Es una forma de organizar el código — por debajo sigue siendo Spark nativo.

# COMMAND ----------

# Esto NO es un UDF — usa la API de Spark por dentro
def add_revenue_flag(dataframe, threshold=1000):
    return dataframe.withColumn(
        "high_revenue",
        F.when(F.col("amount") > threshold, True).otherwise(False)
    )

def add_status_clean(dataframe):
    return dataframe.withColumn(
        "status_clean",
        F.upper(F.trim(F.col("status")))
    )

# Composición de wrappers — todo sigue siendo Spark nativo
result_wrapper = (
    df
    .transform(add_revenue_flag, threshold=1500)
    .transform(add_status_clean)
    .filter(F.col("status_clean") == "COMPLETED")
)

result_wrapper.show(5)

# COMMAND ----------

# Veamos el plan — es Spark puro, optimizado por Catalyst
result_wrapper.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. UDF — Python puro, fila por fila
# MAGIC
# MAGIC Un UDF le pide a Spark que serialice cada fila a Python, ejecute tu función, y serialice el resultado de vuelta.
# MAGIC Esa ida y vuelta (serde) es lo que lo hace lento.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

# Esto SÍ es un UDF — Python puro, fila por fila
@udf(returnType=StringType())
def classify_tier_udf(amount):
    if amount is None:
        return "basic"
    if amount > 3000:
        return "premium"
    elif amount > 1000:
        return "standard"
    return "basic"

@udf(returnType=DoubleType())
def risk_score_udf(amount, channel):
    if amount is None:
        return 0.1
    if amount > 3000 and channel == "online":
        return 0.9
    elif amount > 1000:
        return 0.5
    return 0.1

@udf(returnType=StringType())
def normalize_channel_udf(channel):
    if channel is None:
        return "UNKNOWN"
    c = channel.strip().upper()
    c = re.sub(r"[^A-Z]", "", c)
    return c

@udf(returnType=StringType())
def build_key_udf(customer_id, transaction_date, channel):
    if customer_id is None or transaction_date is None:
        return "UNKNOWN"
    d = str(transaction_date).replace("-", "")
    ch = (channel or "X")[:3].upper()
    return f"{customer_id}_{d}_{ch}"

result_udf = (
    df
    .filter(F.col("status") == "completed")
    .withColumn("tier", classify_tier_udf(F.col("amount")))
    .withColumn("risk_score", risk_score_udf(F.col("amount"), F.col("channel")))
    .withColumn("channel_norm", normalize_channel_udf(F.col("channel")))
    .withColumn("row_key", build_key_udf(F.col("customer_id"), F.col("transaction_date"), F.col("channel")))
)
result_udf.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Benchmark: nativo vs UDF

# COMMAND ----------

import time

def benchmark(label, func, iterations=3):
    times = []
    for _ in range(iterations):
        start = time.time()
        func()
        times.append(time.time() - start)
    avg = sum(times) / len(times)
    print(f"{label}: {avg:.3f}s promedio ({iterations} iteraciones)")
    return avg

# COMMAND ----------

# Misma operación: filter → tier → risk_score → normalize channel → build key → groupBy → agg

# Nativo — Catalyst: whole-stage codegen, predicate pushdown, columnar
t_native = benchmark(
    "Spark nativo",
    lambda: (
        df
        .filter(F.col("status") == "completed")
        .withColumn("tier", F.when(F.col("amount") > 3000, "premium").when(F.col("amount") > 1000, "standard").otherwise("basic"))
        .withColumn("risk_score", F.when((F.col("amount") > 3000) & (F.col("channel") == "online"), 0.9).when(F.col("amount") > 1000, 0.5).otherwise(0.1))
        .withColumn("channel_norm", F.upper(F.regexp_replace(F.trim(F.col("channel")), r"[^a-zA-Z]", "")))
        .withColumn("row_key", F.concat_ws("_", F.col("customer_id"), F.regexp_replace(F.col("transaction_date"), "-", ""), F.upper(F.substring(F.col("channel"), 1, 3))))
        .groupBy("tier")
        .agg(F.avg("risk_score"), F.count("*"), F.count_distinct("channel_norm"))
        .collect()
    )
)

# UDF — serde fila por fila (4 UDFs × 2M filas = 8M serializaciones), rompe codegen
t_udf = benchmark(
    "UDF Python ",
    lambda: (
        df
        .filter(F.col("status") == "completed")
        .withColumn("tier", classify_tier_udf(F.col("amount")))
        .withColumn("risk_score", risk_score_udf(F.col("amount"), F.col("channel")))
        .withColumn("channel_norm", normalize_channel_udf(F.col("channel")))
        .withColumn("row_key", build_key_udf(F.col("customer_id"), F.col("transaction_date"), F.col("channel")))
        .groupBy("tier")
        .agg(F.avg("risk_score"), F.count("*"), F.count_distinct("channel_norm"))
        .collect()
    )
)

print(f"\nUDF es {t_udf / t_native:.1f}x más lento que nativo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pandas UDFs — el punto medio
# MAGIC
# MAGIC Si no te queda otra que usar un UDF, usá Pandas UDFs (vectorizados).
# MAGIC Procesan por batch en vez de fila por fila.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("string")
def classify_tier_pandas(amount: pd.Series) -> pd.Series:
    return pd.cut(
        amount,
        bins=[-float("inf"), 1000, 3000, float("inf")],
        labels=["basic", "standard", "premium"]
    )

@pandas_udf("string")
def normalize_channel_pandas(channel: pd.Series) -> pd.Series:
    return channel.str.strip().str.upper().str.replace(r"[^A-Z]", "", regex=True)

# Procesa por batch (vectorizado)
result_pandas = (
    df
    .filter(F.col("status") == "completed")
    .withColumn("tier", classify_tier_pandas(F.col("amount")))
    .withColumn("channel_norm", normalize_channel_pandas(F.col("channel")))
)
result_pandas.show(5)

# COMMAND ----------

# Benchmark: nativo vs UDF vs Pandas UDF
t_pandas = benchmark(
    "Pandas UDF  ",
    lambda: (
        df
        .filter(F.col("status") == "completed")
        .withColumn("tier", classify_tier_pandas(F.col("amount")))
        .withColumn("channel_norm", normalize_channel_pandas(F.col("channel")))
        .groupBy("tier")
        .agg(F.count("*"), F.count_distinct("channel_norm"))
        .collect()
    )
)

print(f"\nResumen:")
print(f"  Spark nativo: {t_native:.3f}s (baseline)")
print(f"  Pandas UDF:   {t_pandas:.3f}s ({t_pandas / t_native:.1f}x)")
print(f"  UDF Python:   {t_udf:.3f}s ({t_udf / t_native:.1f}x)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC | Tipo | Rendimiento | Cuándo usarlo |
# MAGIC |------|------------|---------------|
# MAGIC | Funciones nativas (`F.when`, `F.col`) | Óptimo | Siempre que sea posible |
# MAGIC | Wrapper functions | Óptimo | Para organizar código — es Spark nativo por dentro |
# MAGIC | Pandas UDF | Bueno | Cuando necesitás lógica Python pero querés batch |
# MAGIC | UDF Python | Lento | Solo si no hay alternativa nativa |
# MAGIC
# MAGIC **Regla:** si tiene equivalente en `pyspark.sql.functions`, usá la función nativa.
