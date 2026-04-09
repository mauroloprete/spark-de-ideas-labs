# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming en Databricks
# MAGIC
# MAGIC Notebook del blog post **[Databricks Tips #4: Structured Streaming](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-03-structured-streaming/)** del blog [Spark de Ideas](https://mauroloprete.github.io/mauroloprete/).
# MAGIC
# MAGIC Aca vas a practicar los conceptos fundamentales de Structured Streaming:
# MAGIC - Generar datos de ejemplo (JSON) para simular una landing zone
# MAGIC - Modos de trigger: `processingTime` vs `availableNow`
# MAGIC - Auto Loader (`cloudFiles`): schema inference, schema evolution, rescue column
# MAGIC - Watermarks y agregaciones por ventana
# MAGIC - `foreachBatch` con MERGE para upserts
# MAGIC - Monitoreo de queries streaming
# MAGIC
# MAGIC > Ejecutable en Databricks Free Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup: parametros y paths
# MAGIC
# MAGIC Definimos las variables que vamos a usar en todo el notebook.
# MAGIC Usamos `/tmp/` para que funcione en cualquier workspace sin necesidad de Volumes ni mounts.

# COMMAND ----------

import os
import json
import time
from datetime import datetime, timedelta
import random

random.seed(42)

# Paths base — usamos /tmp/ para compatibilidad con Free Edition
BASE_PATH = "/tmp/streaming_lab"
LANDING_ZONE = f"{BASE_PATH}/landing"
CHECKPOINTS = f"{BASE_PATH}/checkpoints"
SCHEMA_LOCATION = f"{BASE_PATH}/schema"

# Limpiar el directorio base si existe (para que el lab sea re-ejecutable)
dbutils.fs.rm(BASE_PATH, recurse=True)
dbutils.fs.mkdirs(LANDING_ZONE)
dbutils.fs.mkdirs(CHECKPOINTS)

print(f"Landing zone: {LANDING_ZONE}")
print(f"Checkpoints:  {CHECKPOINTS}")
print(f"Schema:       {SCHEMA_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generar datos de ejemplo
# MAGIC
# MAGIC Escribimos archivos JSON que simulan eventos de transacciones cayendo en una landing zone.
# MAGIC Cada archivo tiene un batch de transacciones con timestamps escalonados.
# MAGIC
# MAGIC Esto simula el patron tipico: un sistema externo deposita archivos en un bucket/volume
# MAGIC y nosotros los procesamos con streaming.

# COMMAND ----------

def generate_batch(batch_id, num_records=50, base_time=None):
    """Genera un batch de transacciones como lista de dicts."""
    if base_time is None:
        base_time = datetime(2026, 4, 1, 10, 0, 0)

    customers = list(range(1, 21))
    channels = ["online", "retail", "wholesale", "partner"]
    statuses = ["completed", "completed", "completed", "pending", "failed"]

    records = []
    for i in range(num_records):
        # Timestamp con algo de variacion (simulamos eventos reales)
        event_time = base_time + timedelta(
            minutes=random.randint(0, 60),
            seconds=random.randint(0, 59)
        )
        records.append({
            "transaction_id": f"txn_{batch_id}_{i:04d}",
            "customer_id": random.choice(customers),
            "amount": round(random.uniform(10, 5000), 2),
            "channel": random.choice(channels),
            "status": random.choice(statuses),
            "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S")
        })
    return records

# COMMAND ----------

# Escribir 3 batches iniciales
for batch_id in range(3):
    base_time = datetime(2026, 4, 1, 10, 0, 0) + timedelta(hours=batch_id)
    records = generate_batch(batch_id, num_records=50, base_time=base_time)

    # Escribir cada registro como una linea JSON (JSON Lines)
    content = "\n".join(json.dumps(r) for r in records)
    file_path = f"{LANDING_ZONE}/batch_{batch_id:03d}.json"
    dbutils.fs.put(file_path, content, overwrite=True)
    print(f"Escrito: {file_path} ({len(records)} registros)")

print(f"\nArchivos en landing zone:")
display(dbutils.fs.ls(LANDING_ZONE))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Lectura basica con readStream
# MAGIC
# MAGIC Antes de meternos con Auto Loader, veamos como funciona `readStream` con JSON.
# MAGIC Esto nos ayuda a entender el modelo: Spark trata un directorio de archivos como un stream infinito.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Schema explicito — en streaming con JSON es buena practica definirlo
txn_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", TimestampType(), True),
])

# readStream — NO ejecuta nada todavia, es lazy
df_stream_basic = (
    spark.readStream
    .format("json")
    .schema(txn_schema)
    .option("multiLine", "false")  # JSON Lines
    .load(LANDING_ZONE)
)

print(f"Es streaming: {df_stream_basic.isStreaming}")
print(f"Schema:")
df_stream_basic.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Escribimos a una tabla Delta para ver los datos procesados.
# MAGIC Usamos `availableNow` para procesar todo lo que hay y parar.

# COMMAND ----------

# Tabla destino
spark.sql("DROP TABLE IF EXISTS streaming_lab_basic")

query_basic = (
    df_stream_basic.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/basic")
    .trigger(availableNow=True)
    .toTable("streaming_lab_basic")
)

query_basic.awaitTermination()
print("Stream basico terminado.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows, COUNT(DISTINCT transaction_id) AS unique_txns
# MAGIC FROM streaming_lab_basic

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Trigger modes: processingTime vs availableNow
# MAGIC
# MAGIC Los modos de trigger definen **cuando** el stream procesa datos nuevos.
# MAGIC
# MAGIC | Modo | Comportamiento | Costo |
# MAGIC |------|---------------|-------|
# MAGIC | `processingTime("10 seconds")` | Micro-batches cada N segundos, el cluster queda corriendo | Alto (cluster siempre prendido) |
# MAGIC | `availableNow=True` | Procesa todo lo pendiente y para | Bajo (prendido solo lo necesario) |
# MAGIC | `once=True` | Un solo micro-batch y para (deprecado) | Bajo |
# MAGIC
# MAGIC **Regla de costo:** si no necesitas latencia sub-minuto, usa `availableNow`.
# MAGIC Con un job de Databricks Workflows que corra cada 5-15 minutos tenes streaming "near real-time"
# MAGIC a una fraccion del costo de un cluster 24/7.

# COMMAND ----------

# Ejemplo con processingTime — el stream queda corriendo
# NOTA: lo paramos despues de unos segundos para no dejar el cluster ocupado
spark.sql("DROP TABLE IF EXISTS streaming_lab_processing_time")

df_stream_pt = (
    spark.readStream
    .format("json")
    .schema(txn_schema)
    .option("multiLine", "false")
    .load(LANDING_ZONE)
)

query_pt = (
    df_stream_pt.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/processing_time")
    .trigger(processingTime="5 seconds")
    .toTable("streaming_lab_processing_time")
)

# Dejamos que corra unos segundos para ver el comportamiento
print("Stream con processingTime corriendo... esperamos 15 segundos para observar.")
time.sleep(15)

# Chequeamos el progreso
progress = query_pt.lastProgress
if progress:
    print(f"Ultimo batch: {progress['batchId']}")
    print(f"Input rows:   {progress['numInputRows']}")

# Paramos el stream
query_pt.stop()
print("Stream detenido.")

# COMMAND ----------

# Ejemplo con availableNow — procesa todo y para solo
spark.sql("DROP TABLE IF EXISTS streaming_lab_available_now")

df_stream_an = (
    spark.readStream
    .format("json")
    .schema(txn_schema)
    .option("multiLine", "false")
    .load(LANDING_ZONE)
)

query_an = (
    df_stream_an.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/available_now")
    .trigger(availableNow=True)
    .toTable("streaming_lab_available_now")
)

query_an.awaitTermination()
print("availableNow terminado — el stream se detuvo automaticamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificamos que ambos modos procesaron la misma cantidad de datos
# MAGIC SELECT 'processingTime' AS modo, COUNT(*) AS filas FROM streaming_lab_processing_time
# MAGIC UNION ALL
# MAGIC SELECT 'availableNow', COUNT(*) FROM streaming_lab_available_now

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Auto Loader (cloudFiles)
# MAGIC
# MAGIC Auto Loader es la forma recomendada de ingestar archivos en Databricks.
# MAGIC Ventajas sobre `readStream.format("json")`:
# MAGIC
# MAGIC - **Descubrimiento eficiente**: usa notificaciones (SQS/Event Grid) o listing optimizado
# MAGIC - **Schema inference**: detecta el schema automaticamente y lo guarda
# MAGIC - **Schema evolution**: si el schema de los archivos cambia, se adapta
# MAGIC - **Rescue column**: columnas nuevas o datos que no matchean el schema van a `_rescued_data`
# MAGIC - **Exactamente una vez**: no reprocesa archivos ya ingestados

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Auto Loader basico con schema inference

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS streaming_lab_autoloader")

df_autoloader = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION}/autoloader")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING_ZONE)
)

# Veamos que schema infiere
print("Schema inferido por Auto Loader:")
df_autoloader.printSchema()

# COMMAND ----------

# Escribir a tabla Delta
query_al = (
    df_autoloader.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/autoloader")
    .trigger(availableNow=True)
    .toTable("streaming_lab_autoloader")
)

query_al.awaitTermination()
print("Auto Loader terminado.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_lab_autoloader LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Schema evolution y rescue column
# MAGIC
# MAGIC Ahora simulamos que el sistema upstream agrega un campo nuevo (`currency`).
# MAGIC Auto Loader puede manejar esto de dos formas:
# MAGIC - **Schema evolution**: detecta el nuevo campo y actualiza el schema
# MAGIC - **Rescue column**: datos que no matchean van a `_rescued_data` (una columna JSON string)

# COMMAND ----------

# Generar un batch con un campo nuevo: "currency"
new_records = []
base_time = datetime(2026, 4, 1, 13, 0, 0)
for i in range(30):
    event_time = base_time + timedelta(minutes=random.randint(0, 60))
    record = {
        "transaction_id": f"txn_new_{i:04d}",
        "customer_id": random.randint(1, 20),
        "amount": round(random.uniform(10, 5000), 2),
        "channel": random.choice(["online", "retail"]),
        "status": "completed",
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "currency": random.choice(["USD", "ARS", "UYU", "BRL"]),  # campo nuevo
    }
    new_records.append(record)

content = "\n".join(json.dumps(r) for r in new_records)
dbutils.fs.put(f"{LANDING_ZONE}/batch_new_schema.json", content, overwrite=True)
print("Escrito batch con campo 'currency' nuevo.")

# COMMAND ----------

# MAGIC %md
# MAGIC Con `cloudFiles.schemaEvolutionMode` en `addNewColumns` (default en Unity Catalog),
# MAGIC Auto Loader puede agregar la columna nueva automaticamente.
# MAGIC
# MAGIC Si preferis no evolucionar el schema, la rescue column (`_rescued_data`)
# MAGIC captura todo lo que no matchea — asi no perdes datos.

# COMMAND ----------

# Leer con rescue column habilitado
spark.sql("DROP TABLE IF EXISTS streaming_lab_rescue")

df_rescue = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION}/rescue")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # datos nuevos van a _rescued_data
    .load(LANDING_ZONE)
)

query_rescue = (
    df_rescue.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/rescue")
    .trigger(availableNow=True)
    .toTable("streaming_lab_rescue")
)

query_rescue.awaitTermination()
print("Auto Loader con rescue column terminado.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Veamos los registros que tienen datos rescatados (el campo "currency")
# MAGIC SELECT transaction_id, amount, _rescued_data
# MAGIC FROM streaming_lab_rescue
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Fijate que `_rescued_data` tiene un JSON string con los campos que no matchearon
# MAGIC el schema original. Esto te da seguridad: nunca perdes datos, aunque el upstream cambie.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Watermarks y agregaciones por ventana
# MAGIC
# MAGIC Cuando haces agregaciones en streaming (contar eventos por ventana de tiempo,
# MAGIC calcular promedios, etc.), necesitas decirle a Spark **cuanto esperar por datos tardios**.
# MAGIC
# MAGIC Eso es el **watermark**: un umbral de tolerancia. Si un evento llega mas tarde que
# MAGIC el watermark, Spark lo descarta.
# MAGIC
# MAGIC Sin watermark, Spark tendria que mantener **todo** el estado en memoria para siempre.
# MAGIC Con watermark, puede limpiar estado viejo y mantener el uso de memoria controlado.

# COMMAND ----------

from pyspark.sql import functions as F

# Leer el stream
df_stream_wm = (
    spark.readStream
    .format("json")
    .schema(txn_schema)
    .option("multiLine", "false")
    .load(LANDING_ZONE)
)

# Agregacion con watermark: ventas por ventana de 30 minutos, tolerando 1 hora de atraso
df_windowed = (
    df_stream_wm
    .withWatermark("event_time", "1 hour")  # tolerar hasta 1 hora de late data
    .groupBy(
        F.window("event_time", "30 minutes"),  # ventanas de 30 min
        "channel"
    )
    .agg(
        F.count("*").alias("num_transactions"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS streaming_lab_windowed")

query_wm = (
    df_windowed.writeStream
    .format("delta")
    .outputMode("append")  # con watermark, append emite resultados finales
    .option("checkpointLocation", f"{CHECKPOINTS}/windowed")
    .trigger(availableNow=True)
    .toTable("streaming_lab_windowed")
)

query_wm.awaitTermination()
print("Agregacion con watermark terminada.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   window.start AS window_start,
# MAGIC   window.end AS window_end,
# MAGIC   channel,
# MAGIC   num_transactions,
# MAGIC   ROUND(total_amount, 2) AS total_amount,
# MAGIC   ROUND(avg_amount, 2) AS avg_amount
# MAGIC FROM streaming_lab_windowed
# MAGIC ORDER BY window_start, channel

# COMMAND ----------

# MAGIC %md
# MAGIC **Que pasa con late data?**
# MAGIC
# MAGIC Supongamos que el watermark es de 1 hora y la ventana mas reciente es 12:00-12:30.
# MAGIC - Un evento con `event_time = 11:30` llega ahora --> se acepta (esta dentro del watermark)
# MAGIC - Un evento con `event_time = 10:00` llega ahora --> se descarta (mas de 1 hora de atraso)
# MAGIC
# MAGIC La clave: el watermark es un **trade-off entre completitud y uso de memoria**.
# MAGIC Mas tiempo = mas completo, pero mas estado en memoria.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. foreachBatch con MERGE: patron de upsert
# MAGIC
# MAGIC `foreachBatch` te da acceso al DataFrame de cada micro-batch como un batch normal.
# MAGIC Esto te permite hacer operaciones que no son posibles en streaming puro,
# MAGIC como un `MERGE INTO` (upsert) contra una tabla Delta.
# MAGIC
# MAGIC **Caso de uso clasico:** tenes una tabla de dimensiones que recibe actualizaciones.
# MAGIC Si el registro ya existe, lo actualizas. Si es nuevo, lo insertas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear tabla destino para el upsert
# MAGIC DROP TABLE IF EXISTS streaming_lab_upsert;
# MAGIC
# MAGIC CREATE TABLE streaming_lab_upsert (
# MAGIC   transaction_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   amount DOUBLE,
# MAGIC   channel STRING,
# MAGIC   status STRING,
# MAGIC   event_time TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC );

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

def upsert_to_delta(micro_batch_df, batch_id):
    """
    Funcion que recibe cada micro-batch y hace un MERGE contra la tabla destino.
    Si el transaction_id ya existe, actualiza. Si no, inserta.
    """
    # Agregar timestamp de actualizacion
    micro_batch_df = micro_batch_df.withColumn("updated_at", F.current_timestamp())

    # Deduplicate dentro del micro-batch (quedarse con el mas reciente)
    micro_batch_df = (
        micro_batch_df
        .withColumn(
            "rn",
            F.row_number().over(
                F.Window.partitionBy("transaction_id").orderBy(F.col("event_time").desc())
            )
        )
        .filter("rn = 1")
        .drop("rn")
    )

    # MERGE INTO
    target_table = DeltaTable.forName(spark, "streaming_lab_upsert")

    (
        target_table.alias("target")
        .merge(
            micro_batch_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedUpdateAll()  # si existe, actualizar todo
        .whenNotMatchedInsertAll()  # si no existe, insertar
        .execute()
    )

    print(f"Batch {batch_id}: {micro_batch_df.count()} registros procesados con MERGE.")

# COMMAND ----------

# Leer el stream y aplicar foreachBatch
df_stream_upsert = (
    spark.readStream
    .format("json")
    .schema(txn_schema)
    .option("multiLine", "false")
    .load(LANDING_ZONE)
)

query_upsert = (
    df_stream_upsert.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", f"{CHECKPOINTS}/upsert")
    .trigger(availableNow=True)
    .start()
)

query_upsert.awaitTermination()
print("foreachBatch con MERGE terminado.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows, COUNT(DISTINCT transaction_id) AS unique_txns
# MAGIC FROM streaming_lab_upsert

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora simulemos actualizaciones: escribimos un batch con transaction_ids que ya existen
# MAGIC pero con montos distintos. El MERGE deberia actualizarlos.

# COMMAND ----------

# Generar batch con transaction_ids que ya existen (actualizaciones)
update_records = []
base_time = datetime(2026, 4, 1, 14, 0, 0)
for i in range(20):
    record = {
        "transaction_id": f"txn_0_{i:04d}",  # IDs que ya existen del batch 0
        "customer_id": random.randint(1, 20),
        "amount": round(random.uniform(5000, 10000), 2),  # montos mas altos
        "channel": "online",
        "status": "completed",
        "event_time": base_time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    update_records.append(record)

content = "\n".join(json.dumps(r) for r in update_records)
dbutils.fs.put(f"{LANDING_ZONE}/batch_updates.json", content, overwrite=True)
print("Escrito batch con actualizaciones.")

# COMMAND ----------

# Re-ejecutar el stream con foreachBatch — procesa solo los archivos nuevos
query_upsert_2 = (
    df_stream_upsert.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", f"{CHECKPOINTS}/upsert")
    .trigger(availableNow=True)
    .start()
)

query_upsert_2.awaitTermination()
print("Segunda corrida del upsert terminada.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que se actualizaron (los montos deberian ser > 5000 para esos IDs)
# MAGIC SELECT transaction_id, amount, status, updated_at
# MAGIC FROM streaming_lab_upsert
# MAGIC WHERE transaction_id LIKE 'txn_0_00%'
# MAGIC ORDER BY transaction_id
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Fijate que el `updated_at` cambio y los montos ahora son mayores a 5000.
# MAGIC El MERGE actualizo los registros existentes sin duplicarlos.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Monitoreo de queries streaming
# MAGIC
# MAGIC Cuando un stream esta corriendo, necesitas saber si esta sano.
# MAGIC Las metricas clave estan en `query.lastProgress` y `query.recentProgress`.

# COMMAND ----------

# Arranquemos un stream con processingTime para poder inspeccionar el progreso
spark.sql("DROP TABLE IF EXISTS streaming_lab_monitor")

df_stream_mon = (
    spark.readStream
    .format("json")
    .schema(txn_schema)
    .option("multiLine", "false")
    .load(LANDING_ZONE)
)

query_monitor = (
    df_stream_mon.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/monitor")
    .trigger(processingTime="10 seconds")
    .toTable("streaming_lab_monitor")
)

# Esperamos que procese al menos un batch
print("Esperando que el stream procese datos...")
time.sleep(20)

# COMMAND ----------

# Inspeccionar el progreso
progress = query_monitor.lastProgress

if progress:
    print("=== Metricas del ultimo batch ===")
    print(f"Batch ID:           {progress['batchId']}")
    print(f"Input rows:         {progress['numInputRows']}")
    print(f"Input rows/sec:     {progress['inputRowsPerSecond']}")
    print(f"Processed rows/sec: {progress['processedRowsPerSecond']}")
    print(f"Batch duration (ms):")
    for phase, duration in progress.get('durationMs', {}).items():
        print(f"  {phase}: {duration} ms")

    print(f"\nSources:")
    for source in progress.get('sources', []):
        print(f"  Start offset: {source.get('startOffset')}")
        print(f"  End offset:   {source.get('endOffset')}")
        print(f"  Num input rows: {source.get('numInputRows')}")
else:
    print("Todavia no hay progreso disponible.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metricas clave para monitorear
# MAGIC
# MAGIC | Metrica | Que te dice | Alerta si... |
# MAGIC |---------|------------|--------------|
# MAGIC | `numInputRows` | Cuantas filas entraron en el ultimo batch | Es 0 por mucho tiempo (no llegan datos) |
# MAGIC | `inputRowsPerSecond` | Velocidad de ingesta | Cae abruptamente |
# MAGIC | `processedRowsPerSecond` | Velocidad de procesamiento | Es menor que `inputRowsPerSecond` (el stream se atrasa) |
# MAGIC | `durationMs.triggerExecution` | Duracion total del batch | Crece sostenidamente (degradacion) |
# MAGIC | `stateOperators.numRowsTotal` | Filas en el state store | Crece sin control (memory pressure) |
# MAGIC
# MAGIC **La regla mas importante:** `processedRowsPerSecond` debe ser >= `inputRowsPerSecond`.
# MAGIC Si no, el stream se esta atrasando y eventualmente se va a quedar sin memoria o espacio en disco.

# COMMAND ----------

# Ver el status del stream
print(f"Stream activo: {query_monitor.isActive}")
print(f"Status: {query_monitor.status}")

# Listar todos los streams activos en el SparkSession
print(f"\nStreams activos en esta sesion:")
for q in spark.streams.active:
    print(f"  - {q.name or q.id}: activo={q.isActive}")

# COMMAND ----------

# Parar el stream de monitoreo
query_monitor.stop()
print("Stream de monitoreo detenido.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Cleanup
# MAGIC
# MAGIC Paramos todos los streams activos, borramos las tablas y los checkpoints.

# COMMAND ----------

# Parar cualquier stream que haya quedado activo
for q in spark.streams.active:
    print(f"Deteniendo stream: {q.id}")
    q.stop()

print("Todos los streams detenidos.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Borrar tablas creadas en el lab
# MAGIC DROP TABLE IF EXISTS streaming_lab_basic;
# MAGIC DROP TABLE IF EXISTS streaming_lab_processing_time;
# MAGIC DROP TABLE IF EXISTS streaming_lab_available_now;
# MAGIC DROP TABLE IF EXISTS streaming_lab_autoloader;
# MAGIC DROP TABLE IF EXISTS streaming_lab_rescue;
# MAGIC DROP TABLE IF EXISTS streaming_lab_windowed;
# MAGIC DROP TABLE IF EXISTS streaming_lab_upsert;
# MAGIC DROP TABLE IF EXISTS streaming_lab_monitor;

# COMMAND ----------

# Limpiar checkpoints y landing zone
dbutils.fs.rm(BASE_PATH, recurse=True)
print(f"Eliminado: {BASE_PATH}")
print("Cleanup completo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumen
# MAGIC
# MAGIC | Concepto | Lo que vimos |
# MAGIC |----------|-------------|
# MAGIC | `readStream` + JSON | Lectura basica de archivos como stream |
# MAGIC | `processingTime` | Micro-batches periodicos (cluster siempre prendido, costo alto) |
# MAGIC | `availableNow` | Procesar todo lo pendiente y parar (ideal para jobs programados) |
# MAGIC | Auto Loader (`cloudFiles`) | Schema inference, schema evolution, rescue column |
# MAGIC | Watermarks | Controlar cuanto esperar por late data en agregaciones |
# MAGIC | `foreachBatch` + MERGE | Patron de upsert (insert/update) con Delta |
# MAGIC | Monitoreo | `lastProgress`, `inputRowsPerSecond`, `processedRowsPerSecond` |
# MAGIC
# MAGIC **Para profundizar:** lee el blog post completo en
# MAGIC [Databricks Tips #4: Structured Streaming](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-03-structured-streaming/).
