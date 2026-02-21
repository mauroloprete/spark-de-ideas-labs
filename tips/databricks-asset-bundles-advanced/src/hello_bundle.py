# Databricks notebook source
# MAGIC %md
# MAGIC # Hello Databricks Asset Bundles!
# MAGIC
# MAGIC Este notebook fue deployado usando DABs (Databricks Asset Bundles).
# MAGIC
# MAGIC > Ejecutable en Databricks Free Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar que el notebook fue deployado por el bundle

# COMMAND ----------

print("Este notebook fue deployado con Databricks Asset Bundles!")
print(f"Notebook path: {dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear datos de ejemplo

# COMMAND ----------

from pyspark.sql import functions as F

data = [
    ("Alice", "ingestion", 150),
    ("Bob", "transformation", 230),
    ("Carol", "serving", 180),
    ("Dave", "ingestion", 310),
    ("Eve", "transformation", 275),
]

df = spark.createDataFrame(data, ["engineer", "domain", "pipelines_managed"])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformaciones con funciones nativas de Spark

# COMMAND ----------

result = (
    df
    .withColumn("seniority",
        F.when(F.col("pipelines_managed") >= 250, "Senior")
         .when(F.col("pipelines_managed") >= 150, "Mid")
         .otherwise("Junior")
    )
    .withColumn("domain_upper", F.upper(F.col("domain")))
    .orderBy(F.col("pipelines_managed").desc())
)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregation por dominio

# COMMAND ----------

summary = (
    result
    .groupBy("domain")
    .agg(
        F.count("*").alias("engineers"),
        F.sum("pipelines_managed").alias("total_pipelines"),
        F.avg("pipelines_managed").alias("avg_pipelines")
    )
    .orderBy(F.col("total_pipelines").desc())
)

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resultado
# MAGIC
# MAGIC Si estás viendo esto, tu bundle se deployó y ejecutó correctamente.
# MAGIC
# MAGIC Ahora probá:
# MAGIC - Modificar la variable `greeting` en `databricks.yml`
# MAGIC - Re-deployar: `databricks bundle deploy`
# MAGIC - Re-ejecutar: `databricks bundle run hello_job`
# MAGIC - Destruir: `databricks bundle destroy`

# COMMAND ----------

print("DABs lab completado!")
