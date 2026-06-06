# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: AutoLoader + File Arrival + Trigger.AvailableNow
# MAGIC
# MAGIC Notebook del blog post [Databricks Tips #8: Jobs & Workflows](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-08-jobs-workflows/).
# MAGIC
# MAGIC En este lab vas a:
# MAGIC 1. Simular la llegada de archivos JSON a un Volume
# MAGIC 2. Ingestar con AutoLoader (`cloudFiles`) y `Trigger.AvailableNow`
# MAGIC 3. Escribir en la tabla bronze con CDF habilitado (la misma que monitorea el table update trigger)
# MAGIC 4. Verificar exactly-once con checkpoints
# MAGIC
# MAGIC > Prerequisito: ejecutar `00_setup_resources.py` primero para crear schema, Volume y tabla.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 0. Setup — Configuracion

# COMMAND ----------

# Configuracion: cambia estos valores segun tu workspace
CATALOG = "dev_bronze"
SCHEMA = "labs"

# Parametro para controlar cleanup (los jobs con trigger lo pasan como "false")
dbutils.widgets.text("cleanup", "true")
CLEANUP = dbutils.widgets.get("cleanup").lower() == "true"

# Setup automatico
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/lab_bronze_vol"
BRONZE_PATH = f"{VOLUME_PATH}/clientes/"
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints/clientes_bronze/"
TARGET_TABLE = f"{FULL_SCHEMA}.lab_clientes_bronze_cdf"

# Limpiar archivos y checkpoints de corridas anteriores (NO la tabla — es compartida)
spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.lab_clientes_validados")
spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.lab_clientes_quarantine")
dbutils.fs.rm(BRONZE_PATH, recurse=True)
dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
dbutils.fs.mkdirs(BRONZE_PATH)

print(f"Catalogo:   {CATALOG}")
print(f"Schema:     {FULL_SCHEMA}")
print(f"Bronze vol: {BRONZE_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print(f"Tabla:      {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Simular llegada de archivos (batch 1)
# MAGIC
# MAGIC En produccion, estos archivos los dejaria un sistema externo (API, FTP, otro pipeline).
# MAGIC Aca los creamos manualmente para simular el file arrival.

# COMMAND ----------

import json

# Batch 1: 3 archivos con datos de clientes
batch_1 = [
    {
        "file": "clientes_20260601_001.json",
        "records": [
            {"id": 1, "nombre": "Ana Garcia", "email": "ana@example.com", "ciudad": "Montevideo", "monto_total": 1500.00},
            {"id": 2, "nombre": "Carlos Lopez", "email": "carlos@example.com", "ciudad": "Buenos Aires", "monto_total": 2300.50},
            {"id": 3, "nombre": "Maria Rodriguez", "email": "maria@example.com", "ciudad": "Santiago", "monto_total": 890.75},
        ]
    },
    {
        "file": "clientes_20260601_002.json",
        "records": [
            {"id": 4, "nombre": "Pedro Martinez", "email": "pedro@example.com", "ciudad": "Lima", "monto_total": 3100.00},
            {"id": 5, "nombre": "Laura Fernandez", "email": None, "ciudad": "Bogota", "monto_total": 450.25},
        ]
    },
    {
        "file": "clientes_20260602_001.json",
        "records": [
            {"id": 6, "nombre": "Diego Sanchez", "email": "diego@example.com", "ciudad": "Montevideo", "monto_total": 1750.00},
            {"id": 7, "nombre": "Sofia Morales", "email": "sofia@example.com", "ciudad": "Quito", "monto_total": 620.30},
        ]
    }
]

# Escribir archivos JSON en el Volume (un JSON por linea = JSON Lines)
for batch in batch_1:
    file_path = f"{BRONZE_PATH}{batch['file']}"
    content = "\n".join(json.dumps(r) for r in batch["records"])
    dbutils.fs.put(file_path, content, overwrite=True)
    print(f"Archivo creado: {batch['file']} ({len(batch['records'])} registros)")

print(f"\nTotal batch 1: {sum(len(b['records']) for b in batch_1)} registros en {len(batch_1)} archivos")

# COMMAND ----------

# Verificar que los archivos estan en el Volume
display(dbutils.fs.ls(BRONZE_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Ingestar con AutoLoader + AvailableNow
# MAGIC
# MAGIC AutoLoader (`cloudFiles`) descubre archivos nuevos incrementalmente.
# MAGIC `Trigger.AvailableNow` procesa **todo** lo pendiente y termina.
# MAGIC
# MAGIC Escribe en `lab_clientes_bronze_cdf` — la misma tabla que monitorea el table update trigger.
# MAGIC Cada escritura dispara automaticamente el Job de microbatch (Lab 2).

# COMMAND ----------

query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(BRONZE_PATH)
    .writeStream
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

# Esperar a que termine (AvailableNow termina solo)
query.awaitTermination()
print("Stream finalizado — todos los archivos procesados")
print("(Si el table update trigger esta activo, el Job de microbatch se disparo automaticamente)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar los datos ingestados

# COMMAND ----------

display(spark.table(TARGET_TABLE).orderBy("id"))

# COMMAND ----------

print(f"Total registros: {spark.table(TARGET_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Simular segunda llegada de archivos (batch 2)
# MAGIC
# MAGIC Ahora llegan mas archivos. El checkpoint de AutoLoader sabe cuales ya proceso.
# MAGIC Al re-ejecutar, solo procesa los **nuevos** — esto es exactly-once.

# COMMAND ----------

batch_2 = [
    {
        "file": "clientes_20260603_001.json",
        "records": [
            {"id": 8, "nombre": "Valeria Ruiz", "email": "valeria@example.com", "ciudad": "Montevideo", "monto_total": 980.00},
            {"id": 9, "nombre": "Andres Torres", "email": "andres@example.com", "ciudad": "Asuncion", "monto_total": 1200.50},
            {"id": 10, "nombre": "Camila Herrera", "email": "camila@example.com", "ciudad": "Caracas", "monto_total": 340.80},
        ]
    },
    {
        "file": "clientes_20260603_002.json",
        "records": [
            {"id": 11, "nombre": "Roberto Diaz", "email": None, "ciudad": "La Paz", "monto_total": 2100.00},
            {"id": 12, "nombre": "Lucia Vargas", "email": "lucia@example.com", "ciudad": "San Jose", "monto_total": 1550.75},
        ]
    }
]

for batch in batch_2:
    file_path = f"{BRONZE_PATH}{batch['file']}"
    content = "\n".join(json.dumps(r) for r in batch["records"])
    dbutils.fs.put(file_path, content, overwrite=True)
    print(f"Archivo creado: {batch['file']} ({len(batch['records'])} registros)")

print(f"\nTotal batch 2: {sum(len(b['records']) for b in batch_2)} registros en {len(batch_2)} archivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Re-ejecutar AutoLoader
# MAGIC
# MAGIC Mismo codigo que antes. El checkpoint hace que **solo procese los archivos nuevos**.
# MAGIC En produccion, el file arrival trigger dispara el Job automaticamente.

# COMMAND ----------

query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(BRONZE_PATH)
    .writeStream
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()
print("Stream finalizado — solo archivos nuevos procesados")

# COMMAND ----------

# Deberian ser 12 registros: 7 del batch 1 + 5 del batch 2
print(f"Total registros: {spark.table(TARGET_TABLE).count()}")

# COMMAND ----------

display(spark.table(TARGET_TABLE).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Variante: foreachBatch con validacion
# MAGIC
# MAGIC Si necesitas logica custom (validar, separar buenos/malos, llamar una API), usa `foreachBatch`.
# MAGIC
# MAGIC **Ojo**: `foreachBatch` da **at-least-once**, no exactly-once. Disena tu logica para ser idempotente.

# COMMAND ----------

VALIDADOS_TABLE = f"{FULL_SCHEMA}.lab_clientes_validados"
QUARANTINE_TABLE = f"{FULL_SCHEMA}.lab_clientes_quarantine"

def procesar_batch(batch_df, batch_id):
    """Separa registros validos de invalidos."""
    # Validos: tienen email
    df_valido = batch_df.filter("email IS NOT NULL")
    # Invalidos: sin email → quarantine
    df_invalido = batch_df.filter("email IS NULL")

    n_validos = df_valido.count()
    n_invalidos = df_invalido.count()

    if n_validos > 0:
        df_valido.write.mode("append").saveAsTable(VALIDADOS_TABLE)

    if n_invalidos > 0:
        df_invalido.write.mode("append").saveAsTable(QUARANTINE_TABLE)

    print(f"Batch {batch_id}: {n_validos} validos, {n_invalidos} a quarantine")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {VALIDADOS_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {QUARANTINE_TABLE}")

# Usamos un checkpoint diferente para este stream
CHECKPOINT_FOREACH = f"{VOLUME_PATH}/_checkpoints/clientes_foreach/"
dbutils.fs.rm(CHECKPOINT_FOREACH, recurse=True)

query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_FOREACH)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(BRONZE_PATH)
    .writeStream
    .foreachBatch(procesar_batch)
    .option("checkpointLocation", CHECKPOINT_FOREACH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("\nforeachBatch finalizado")

# COMMAND ----------

# Registros validos vs quarantine
print(f"Validos:    {spark.table(VALIDADOS_TABLE).count()}")
print(f"Quarantine: {spark.table(QUARANTINE_TABLE).count()}")

# COMMAND ----------

# Ver los registros en quarantine (sin email)
display(spark.table(QUARANTINE_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Comparar AvailableNow vs ProcessingTime
# MAGIC
# MAGIC Para entender la diferencia, veamos que pasa con `ProcessingTime`:
# MAGIC
# MAGIC - `availableNow=True` → procesa todo lo pendiente y **termina**. Perfecto para Jobs.
# MAGIC - `processingTime="10 seconds"` → ejecuta micro-batches cada 10s y **nunca termina**. El Job Cluster queda vivo (y pagando).
# MAGIC - `once=True` → procesa **un solo micro-batch** y termina. Si hay 10.000 archivos, procesa un batch y deja el resto. **Deprecado**.
# MAGIC
# MAGIC En un Job con file arrival trigger, siempre usa `availableNow=True`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Cleanup
# MAGIC
# MAGIC Borrar tablas auxiliares, archivos y checkpoints del lab.
# MAGIC La tabla `lab_clientes_bronze_cdf` NO se borra (es compartida con el table update trigger).
# MAGIC Si el notebook corre desde un Job con trigger, el parametro `cleanup=false` desactiva esta seccion.

# COMMAND ----------

if CLEANUP:
    spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.lab_clientes_validados")
    spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.lab_clientes_quarantine")

    dbutils.fs.rm(BRONZE_PATH, recurse=True)
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    dbutils.fs.rm(CHECKPOINT_FOREACH, recurse=True)
    dbutils.fs.mkdirs(BRONZE_PATH)
    print("Tablas auxiliares, archivos y checkpoints eliminados")
    print("Nota: lab_clientes_bronze_cdf NO se borra (compartida con Lab 2 y triggers)")
else:
    print("Cleanup deshabilitado (cleanup=false). Recursos mantenidos para triggers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Pasa al **Lab 2** (`02_microbatch_table_update.py`) para armar el pipeline bronze → silver con Change Data Feed y table update triggers.
# MAGIC
# MAGIC O espera — si el table update trigger esta activo, el Lab 2 ya se disparo automaticamente cuando este notebook escribio en `lab_clientes_bronze_cdf`.
