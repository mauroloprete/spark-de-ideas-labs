# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Crear recursos para los triggers
# MAGIC
# MAGIC Este notebook crea el schema, Volume y tabla bronze que necesitan
# MAGIC los Jobs con file arrival trigger y table update trigger.
# MAGIC
# MAGIC Ejecutar ANTES de activar los triggers.

# COMMAND ----------

CATALOG = "dev_bronze"
SCHEMA = "labs"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FULL_SCHEMA}.lab_bronze_vol")

# Crear directorio monitoreado por file arrival trigger
BRONZE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/lab_bronze_vol/clientes/"
dbutils.fs.mkdirs(BRONZE_PATH)

print(f"Schema:  {FULL_SCHEMA}")
print(f"Volume:  /Volumes/{CATALOG}/{SCHEMA}/lab_bronze_vol")
print(f"Bronze: {BRONZE_PATH}")

# COMMAND ----------

# Tabla bronze con CDF (monitoreada por table update trigger)
BRONZE_TABLE = f"{FULL_SCHEMA}.lab_clientes_bronze_cdf"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
  id INT,
  nombre STRING,
  email STRING,
  ciudad STRING,
  monto_total DECIMAL(10, 2)
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print(f"Tabla:   {BRONZE_TABLE} (con CDF, vacia)")
print()
print("Recursos listos.")
print("  1. Activa los triggers desde la UI o con bundle deploy")
print("  2. Ejecuta Lab 1 (autoloader) para poblar bronze")
print("  3. El table update trigger dispara Lab 2 (microbatch) automaticamente")
