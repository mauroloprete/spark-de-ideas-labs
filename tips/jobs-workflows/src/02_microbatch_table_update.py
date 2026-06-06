# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Microbatch bronze → silver con Table Update + CDF
# MAGIC
# MAGIC Notebook del blog post [Databricks Tips #8: Jobs & Workflows](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-08-jobs-workflows/).
# MAGIC
# MAGIC En este lab vas a:
# MAGIC 1. Leer los cambios de la tabla bronze (poblada por Lab 1) con `readStream` + `readChangeFeed`
# MAGIC 2. Transformar y escribir en silver con `AvailableNow`
# MAGIC 3. Simular updates en bronze y re-procesar
# MAGIC 4. Usar `foreachBatch` + MERGE para evitar duplicados
# MAGIC
# MAGIC En produccion, este notebook se dispara automaticamente con un **table update trigger**
# MAGIC que monitorea `lab_clientes_bronze_cdf`.
# MAGIC
# MAGIC > Prerequisito: ejecutar `00_setup_resources.py` y `01_autoloader_file_arrival.py` primero.

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

# Tabla bronze (creada por setup, poblada por Lab 1)
BRONZE_TABLE = f"{FULL_SCHEMA}.lab_clientes_bronze_cdf"
SILVER_TABLE = f"{FULL_SCHEMA}.lab_clientes_silver"
SILVER_MERGED_TABLE = f"{FULL_SCHEMA}.lab_clientes_silver_merged"

# Limpiar tablas silver de corridas anteriores (NO bronze — es compartida)
spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SILVER_MERGED_TABLE}")

print(f"Catalogo: {CATALOG}")
print(f"Schema:   {FULL_SCHEMA}")
print(f"Bronze:   {BRONZE_TABLE}")
print(f"Silver:   {SILVER_TABLE}")

# COMMAND ----------

# Verificar que la tabla bronze existe y tiene datos (del Lab 1)
count = spark.table(BRONZE_TABLE).count()
print(f"Registros en bronze: {count}")
if count == 0:
    print("ATENCION: la tabla bronze esta vacia. Ejecuta Lab 1 primero.")

# COMMAND ----------

display(spark.table(BRONZE_TABLE).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Ver el Change Data Feed
# MAGIC
# MAGIC CDF registra **que cambio** en cada commit: inserts, updates (pre/post image), deletes.
# MAGIC Podemos leerlo con `table_changes()` en SQL o `readChangeFeed` en PySpark.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM table_changes('{BRONZE_TABLE}', 0) ORDER BY _commit_version, id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Las columnas que agrega CDF:
# MAGIC - `_change_type`: `insert`, `update_preimage`, `update_postimage`, `delete`
# MAGIC - `_commit_version`: version del commit Delta
# MAGIC - `_commit_timestamp`: timestamp del commit

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Pipeline silver con readStream + CDF + AvailableNow
# MAGIC
# MAGIC Leemos la tabla bronze como stream con `readChangeFeed = true`.
# MAGIC Solo procesamos inserts y updates (postimage), descartamos deletes y preimages.
# MAGIC
# MAGIC En produccion, un **table update trigger** dispara este Job cuando bronze cambia.
# MAGIC El Job Cluster se crea, ejecuta esto, y se destruye.

# COMMAND ----------

from pyspark.sql import functions as F

CHECKPOINT_SILVER = f"{VOLUME_PATH}/_checkpoints/clientes_silver/"
dbutils.fs.rm(CHECKPOINT_SILVER, recurse=True)

# COMMAND ----------

query = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table(BRONZE_TABLE)
    # Solo inserts y updates (postimage)
    .filter("_change_type IN ('insert', 'update_postimage')")
    # Descartar columnas de CDF (ya no las necesitamos en silver)
    .drop("_change_type", "_commit_version", "_commit_timestamp")
    # Transformaciones silver
    .withColumn("nombre_upper", F.upper(F.col("nombre")))
    .withColumn("ciudad_upper", F.upper(F.col("ciudad")))
    .withColumn("categoria_monto",
        F.when(F.col("monto_total") >= 2000, "premium")
         .when(F.col("monto_total") >= 1000, "standard")
         .otherwise("basic")
    )
    .withColumn("procesado_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

query.awaitTermination()
print("Pipeline silver finalizado")

# COMMAND ----------

display(spark.table(SILVER_TABLE).orderBy("id"))

# COMMAND ----------

# Verificar la clasificacion por monto
display(spark.sql(f"""
SELECT categoria_monto, count(*) AS total, round(avg(monto_total), 2) AS monto_promedio
FROM {SILVER_TABLE}
GROUP BY categoria_monto
ORDER BY monto_promedio DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Simular cambios en bronze
# MAGIC
# MAGIC Ahora simulamos lo que pasaria en produccion: llegan datos nuevos y se actualizan existentes.
# MAGIC En produccion, el table update trigger detecta estos cambios y dispara el Job de silver.

# COMMAND ----------

# Nuevos clientes (INSERT)
spark.sql(f"""
INSERT INTO {BRONZE_TABLE} (id, nombre, email, ciudad, monto_total)
VALUES
  (13, 'Fernando Gutierrez', 'fernando@example.com', 'Montevideo', 1750.00),
  (14, 'Natalia Mendez', 'natalia@example.com', 'Quito', 620.30),
  (15, 'Gabriel Rojas', 'gabriel@example.com', 'Lima', 2980.00)
""")

# Actualizar monto de un cliente existente (UPDATE → genera preimage + postimage en CDF)
spark.sql(f"UPDATE {BRONZE_TABLE} SET monto_total = 5200.00 WHERE id = 2")

print("Cambios aplicados: 3 inserts + 1 update")

# COMMAND ----------

# Ver los cambios nuevos en CDF (versiones recientes)
latest_version = spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE} LIMIT 1").select("version").collect()[0][0]
display(spark.sql(f"""
SELECT _change_type, id, nombre, monto_total, _commit_version
FROM table_changes('{BRONZE_TABLE}', {latest_version - 1})
ORDER BY _commit_version, _change_type, id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Fijate que el UPDATE de Carlos Lopez genera dos filas:
# MAGIC - `update_preimage`: el valor anterior (2300.50)
# MAGIC - `update_postimage`: el valor nuevo (5200.00)
# MAGIC
# MAGIC Nuestro pipeline silver solo toma `insert` y `update_postimage`, asi que Carlos va a tener el monto actualizado.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Re-ejecutar pipeline silver
# MAGIC
# MAGIC Mismo codigo que antes. El checkpoint hace que solo procese los **cambios nuevos**.
# MAGIC En produccion, el table update trigger dispara esto automaticamente.

# COMMAND ----------

query = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table(BRONZE_TABLE)
    .filter("_change_type IN ('insert', 'update_postimage')")
    .drop("_change_type", "_commit_version", "_commit_timestamp")
    .withColumn("nombre_upper", F.upper(F.col("nombre")))
    .withColumn("ciudad_upper", F.upper(F.col("ciudad")))
    .withColumn("categoria_monto",
        F.when(F.col("monto_total") >= 2000, "premium")
         .when(F.col("monto_total") >= 1000, "standard")
         .otherwise("basic")
    )
    .withColumn("procesado_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

query.awaitTermination()
print("Pipeline silver re-ejecutado — solo cambios nuevos procesados")

# COMMAND ----------

# Carlos Lopez puede aparecer 2 veces (append mode: original + update_postimage)
display(spark.table(SILVER_TABLE).orderBy("id", "procesado_at"))

# COMMAND ----------

print(f"Total filas: {spark.table(SILVER_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atencion: duplicados en updates
# MAGIC
# MAGIC Si Carlos Lopez aparece 2 veces (la version original y la actualizada), es porque estamos usando **append mode**.
# MAGIC El `update_postimage` se agrega como una fila nueva.
# MAGIC
# MAGIC En produccion, tenes dos opciones para manejar esto:
# MAGIC
# MAGIC 1. **`foreachBatch` + MERGE**: usa `MERGE INTO` para hacer upsert por `id`. Mas complejo pero sin duplicados.
# MAGIC 2. **Dedup en la capa gold**: deja silver con append y deduplica en gold con `ROW_NUMBER() OVER (PARTITION BY id ORDER BY procesado_at DESC)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Bonus: foreachBatch con MERGE (sin duplicados)
# MAGIC
# MAGIC Para evitar duplicados, usamos `foreachBatch` con `MERGE INTO`.
# MAGIC Esto es mas robusto para tablas con updates frecuentes.

# COMMAND ----------

from delta.tables import DeltaTable

# Crear tabla silver para merge
spark.sql(f"""
CREATE TABLE {SILVER_MERGED_TABLE} (
  id INT,
  nombre STRING,
  email STRING,
  ciudad STRING,
  monto_total DECIMAL(10, 2),
  nombre_upper STRING,
  ciudad_upper STRING,
  categoria_monto STRING,
  procesado_at TIMESTAMP
) USING DELTA
""")

CHECKPOINT_MERGE = f"{VOLUME_PATH}/_checkpoints/clientes_silver_merge/"
dbutils.fs.rm(CHECKPOINT_MERGE, recurse=True)

def upsert_to_silver(batch_df, batch_id):
    """MERGE: inserta nuevos, actualiza existentes por id."""
    transformed = (batch_df
        .filter("_change_type IN ('insert', 'update_postimage')")
        .drop("_change_type", "_commit_version", "_commit_timestamp")
        .withColumn("nombre_upper", F.upper(F.col("nombre")))
        .withColumn("ciudad_upper", F.upper(F.col("ciudad")))
        .withColumn("categoria_monto",
            F.when(F.col("monto_total") >= 2000, "premium")
             .when(F.col("monto_total") >= 1000, "standard")
             .otherwise("basic")
        )
        .withColumn("procesado_at", F.current_timestamp())
    )

    if transformed.count() == 0:
        return

    target = DeltaTable.forName(spark, SILVER_MERGED_TABLE)
    (target.alias("t")
        .merge(transformed.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Batch {batch_id}: {transformed.count()} filas merged")

# COMMAND ----------

query = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table(BRONZE_TABLE)
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", CHECKPOINT_MERGE)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("MERGE finalizado")

# COMMAND ----------

# Sin duplicados: Carlos Lopez tiene el monto actualizado (5200.00)
display(spark.table(SILVER_MERGED_TABLE).orderBy("id"))

# COMMAND ----------

# Sin duplicados — cada id aparece una sola vez
print(f"Total filas: {spark.table(SILVER_MERGED_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Parametros dinamicos del table update trigger
# MAGIC
# MAGIC Cuando un table update trigger dispara el Job, Databricks inyecta parametros que podes usar.
# MAGIC En este lab los simulamos, pero en produccion se llenan automaticamente.

# COMMAND ----------

# En produccion, estos parametros los inyecta el trigger:
# updated_tables = dbutils.widgets.get("job.trigger.table_update.updated_tables")
# commit_ts = dbutils.widgets.get(f"job.trigger.table_update.{BRONZE_TABLE}.commit_timestamp.iso_datetime")
# version = dbutils.widgets.get(f"job.trigger.table_update.{BRONZE_TABLE}.version")

print("Parametros que inyecta el table update trigger:")
print(f'  updated_tables:     ["{BRONZE_TABLE}"]')
print('  commit_timestamp:   2026-06-04T10:30:00.000Z')
print('  version:            3')
print()
print("Estos parametros te permiten:")
print("  - Saber que tablas cambiaron (si monitoreás varias)")
print("  - Hacer procesamiento incremental por version")
print("  - Logging y auditoria")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Cleanup
# MAGIC
# MAGIC Borrar tablas silver y checkpoints.
# MAGIC La tabla `lab_clientes_bronze_cdf` NO se borra (es compartida con Lab 1 y los triggers).

# COMMAND ----------

if CLEANUP:
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_MERGED_TABLE}")

    dbutils.fs.rm(f"{VOLUME_PATH}/_checkpoints/clientes_silver/", recurse=True)
    dbutils.fs.rm(f"{VOLUME_PATH}/_checkpoints/clientes_silver_merge/", recurse=True)
    print("Tablas silver y checkpoints eliminados")
    print("Nota: lab_clientes_bronze_cdf NO se borra (compartida con Lab 1 y triggers)")
else:
    print("Cleanup deshabilitado (cleanup=false). Recursos mantenidos para triggers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumen
# MAGIC
# MAGIC | Concepto | Que aprendiste |
# MAGIC |---|---|
# MAGIC | **AutoLoader** | Ingesta incremental con `cloudFiles`, descubre archivos nuevos automaticamente |
# MAGIC | **AvailableNow** | Procesa todo lo pendiente y termina — perfecto para Jobs con Job Cluster |
# MAGIC | **Change Data Feed** | Lee solo los cambios (inserts, updates, deletes) de una tabla Delta |
# MAGIC | **foreachBatch + MERGE** | Upsert sin duplicados para tablas con updates frecuentes |
# MAGIC | **Table Update Trigger** | Dispara Jobs cuando una tabla cambia (event-driven, sin cluster 24/7) |
# MAGIC | **File Arrival Trigger** | Dispara Jobs cuando llegan archivos nuevos a un Volume |
# MAGIC
# MAGIC ### Pipeline conectado
# MAGIC
# MAGIC ```
# MAGIC Archivos → Volume → File Arrival Trigger → Lab 1 (AutoLoader) → bronze_cdf → Table Update Trigger → Lab 2 (Microbatch) → silver
# MAGIC ```
# MAGIC
# MAGIC Lee el blog post completo: [Databricks Tips #8](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-08-jobs-workflows/)
