# Databricks notebook source
# MAGIC %md
# MAGIC # Patrones de Ingesta en Data Engineering
# MAGIC
# MAGIC Lab del blog post [Data Engineering Design Patterns: 8 patrones de ingesta](https://mauroloprete.github.io/mauroloprete/blog/posts/data-engineering-design-patterns/).
# MAGIC
# MAGIC En este notebook implementamos 4 de los patrones de ingesta mas usados en produccion:
# MAGIC
# MAGIC 1. **Full Loader** -- carga completa, sobreescribir todo
# MAGIC 2. **Incremental Loader** -- alta marca de agua (high watermark)
# MAGIC 3. **CDC con MERGE INTO** -- captura de cambios (insert, update, delete)
# MAGIC 4. **Compactor (OPTIMIZE)** -- consolidar archivos chicos
# MAGIC
# MAGIC > Ejecutable en Databricks Free Edition con Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup: datos de ejemplo
# MAGIC
# MAGIC Simulamos un sistema transaccional (ERP) con una tabla de clientes y una tabla de ordenes.
# MAGIC Todo se crea en un schema dedicado para no contaminar nada.

# COMMAND ----------

# Configuracion del lab
CATALOG = "workspace"
SCHEMA = "ingestion_patterns_lab"

# Limpiar schema si existe de una corrida anterior (para que sea re-ejecutable)
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
spark.sql(f"CREATE SCHEMA {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Schema: {CATALOG}.{SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType
)
from datetime import datetime, timedelta
import random

random.seed(42)

# --- Tabla fuente: clientes (tabla chica, tipo dimension) ---
customers_data = [
    (1, "Luciana Gomez", "Buenos Aires", "2024-01-15"),
    (2, "Santiago Perez", "Montevideo", "2024-02-20"),
    (3, "Valentina Rodriguez", "Cordoba", "2024-03-10"),
    (4, "Mateo Fernandez", "Rosario", "2024-04-05"),
    (5, "Camila Lopez", "Mendoza", "2024-05-12"),
    (6, "Joaquin Martinez", "La Plata", "2024-06-18"),
    (7, "Martina Silva", "Montevideo", "2024-07-22"),
    (8, "Thiago Gonzalez", "Tucuman", "2024-08-30"),
]

customers_df = spark.createDataFrame(
    customers_data,
    ["customer_id", "name", "city", "registered_at"]
)

customers_df.write.format("delta").mode("overwrite").saveAsTable("source_customers")
print("Tabla source_customers creada:")
spark.table("source_customers").show(truncate=False)

# COMMAND ----------

# --- Tabla fuente: ordenes (tabla transaccional con updated_at) ---
base_date = datetime(2025, 6, 1, 8, 0, 0)
orders_data = []

for i in range(1, 51):
    orders_data.append((
        i,
        random.choice([1, 2, 3, 4, 5, 6, 7, 8]),
        round(random.uniform(50.0, 2000.0), 2),
        random.choice(["PENDING", "COMPLETED", "SHIPPED"]),
        (base_date + timedelta(hours=random.randint(0, 240))).strftime("%Y-%m-%d %H:%M:%S"),
        (base_date + timedelta(hours=random.randint(0, 240))).strftime("%Y-%m-%d %H:%M:%S"),
    ))

orders_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

orders_df = (
    spark.createDataFrame(orders_data, orders_schema)
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)

orders_df.write.format("delta").mode("overwrite").saveAsTable("source_orders")

print("Tabla source_orders creada (50 filas):")
spark.table("source_orders").orderBy("order_id").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Full Loader
# MAGIC
# MAGIC El patron mas simple: leer **toda** la tabla fuente y sobreescribir el destino.
# MAGIC
# MAGIC **Cuando usarlo:**
# MAGIC - Tablas de referencia chicas (paises, monedas, configuraciones)
# MAGIC - Fuentes sin columna de fecha de modificacion
# MAGIC - Cuando necesitas garantizar consistencia total sin complicarte
# MAGIC
# MAGIC **El costo:** si la tabla crece, re-lees todo cada vez. Para una tabla de 8 filas no importa;
# MAGIC para una de 800 millones, es inviable.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Primera carga completa

# COMMAND ----------

# Full Loader: leer todo, escribir todo
df_source = spark.table("source_customers")

(df_source.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_customers_full"))

print("Full Load completado:")
spark.table("bronze_customers_full").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Agregar un cliente a la fuente y re-ejecutar
# MAGIC
# MAGIC Esto demuestra el punto: el Full Loader no distingue registros nuevos de existentes.
# MAGIC Lee todo de vuelta.

# COMMAND ----------

# Agregar un nuevo cliente a la fuente
new_customer = spark.createDataFrame(
    [(9, "Federico Alvarez", "Salta", "2025-01-10")],
    ["customer_id", "name", "city", "registered_at"]
)

new_customer.write.format("delta").mode("append").saveAsTable("source_customers")

# Full Load de nuevo -- lee las 9 filas, no solo la nueva
df_source = spark.table("source_customers")

(df_source.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_customers_full"))

count = spark.table("bronze_customers_full").count()
print(f"Full Load: se leyeron y escribieron {count} filas (todas, no solo la nueva)")
spark.table("bronze_customers_full").orderBy("customer_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Incremental Loader (High Watermark)
# MAGIC
# MAGIC Solo trae los registros nuevos o modificados desde la ultima ejecucion.
# MAGIC Requiere una **columna de control** confiable (timestamp, ID autoincremental, etc.).
# MAGIC
# MAGIC El concepto clave es la **marca de agua** (watermark): guardas el maximo valor de la
# MAGIC columna de control que ya procesaste, y la proxima vez lees solo lo que es mayor a eso.
# MAGIC
# MAGIC **Cuando usarlo:**
# MAGIC - Tablas transaccionales que crecen constantemente
# MAGIC - Tienen una columna `updated_at` o similar confiable
# MAGIC - Es el patron por defecto en la mayoria de los pipelines batch

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Crear tabla de control para watermarks

# COMMAND ----------

# Tabla de control: guarda el ultimo watermark por cada tabla
spark.sql("""
    CREATE OR REPLACE TABLE watermark_control (
        table_name STRING,
        last_watermark TIMESTAMP,
        last_run_at TIMESTAMP
    )
""")

print("Tabla watermark_control creada")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Primera carga incremental (carga inicial)

# COMMAND ----------

def incremental_load(source_table, target_table, watermark_column):
    """
    Carga incremental usando high watermark.
    Si no hay watermark previo, trae todo (carga inicial).
    """
    # Leer el ultimo watermark
    wm_row = spark.sql(f"""
        SELECT last_watermark
        FROM watermark_control
        WHERE table_name = '{target_table}'
    """).collect()

    if len(wm_row) > 0 and wm_row[0][0] is not None:
        last_wm = wm_row[0][0]
        print(f"Watermark anterior: {last_wm}")
        df_new = (
            spark.table(source_table)
            .filter(F.col(watermark_column) > F.lit(last_wm))
        )
    else:
        print("Sin watermark previo -- carga inicial completa")
        df_new = spark.table(source_table)

    new_count = df_new.count()
    if new_count == 0:
        print("No hay registros nuevos. Nada que cargar.")
        return

    print(f"Registros nuevos a cargar: {new_count}")

    # Escribir en modo append
    df_new.write.format("delta").mode("append").saveAsTable(target_table)

    # Calcular y guardar el nuevo watermark
    new_wm = df_new.agg(F.max(watermark_column)).collect()[0][0]

    spark.sql(f"""
        MERGE INTO watermark_control AS target
        USING (
            SELECT
                '{target_table}' AS table_name,
                TIMESTAMP '{new_wm}' AS last_watermark,
                current_timestamp() AS last_run_at
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Nuevo watermark: {new_wm}")


# Primera carga: trae todo porque no hay watermark
incremental_load("source_orders", "bronze_orders_inc", "updated_at")

# COMMAND ----------

print("Estado despues de la carga inicial:")
print(f"Filas en bronze: {spark.table('bronze_orders_inc').count()}")
spark.table("watermark_control").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Agregar registros nuevos a la fuente y cargar incrementalmente

# COMMAND ----------

# Simular nuevas ordenes que llegan al sistema fuente
# Usamos el schema de la tabla fuente para evitar conflictos de tipos
new_orders = spark.createDataFrame(
    [
        (51, 1, 1500.00, "COMPLETED",
         datetime(2025, 6, 15, 10, 0, 0), datetime(2025, 6, 15, 10, 0, 0)),
        (52, 3, 750.50, "PENDING",
         datetime(2025, 6, 15, 11, 30, 0), datetime(2025, 6, 15, 11, 30, 0)),
        (53, 5, 2200.00, "SHIPPED",
         datetime(2025, 6, 15, 14, 0, 0), datetime(2025, 6, 15, 14, 0, 0)),
    ],
    spark.table("source_orders").schema
)

new_orders.write.format("delta").mode("append").saveAsTable("source_orders")
print("3 ordenes nuevas agregadas a source_orders")

# COMMAND ----------

# Segunda carga incremental: solo trae las 3 nuevas
incremental_load("source_orders", "bronze_orders_inc", "updated_at")

# COMMAND ----------

print(f"Total de filas en bronze: {spark.table('bronze_orders_inc').count()}")
print("\nUltimos registros cargados:")
spark.table("bronze_orders_inc").orderBy(F.desc("updated_at")).show(5, truncate=False)

print("Watermark actualizado:")
spark.table("watermark_control").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Ejecutar de nuevo sin datos nuevos
# MAGIC
# MAGIC Si no hay registros nuevos, no se escribe nada. El watermark queda igual.

# COMMAND ----------

incremental_load("source_orders", "bronze_orders_inc", "updated_at")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. CDC con MERGE INTO
# MAGIC
# MAGIC **Change Data Capture**: captura los cambios (inserts, updates, deletes) del sistema fuente
# MAGIC y los aplica al destino usando `MERGE INTO`.
# MAGIC
# MAGIC Es el patron mas potente para fuentes transaccionales porque captura **todo** tipo de cambio,
# MAGIC incluyendo deletes que el Incremental Loader no ve.
# MAGIC
# MAGIC En produccion, herramientas como Debezium o Fivetran generan los eventos CDC.
# MAGIC Aca los simulamos a mano para entender la mecanica.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Crear tabla destino Silver y simular primer batch de CDC

# COMMAND ----------

# Crear tabla silver con los clientes iniciales
spark.sql("""
    CREATE OR REPLACE TABLE silver_customers (
        customer_id INT,
        name STRING,
        city STRING,
        registered_at STRING
    )
""")

# Insertar estado inicial
spark.sql("""
    INSERT INTO silver_customers
    SELECT customer_id, name, city, registered_at
    FROM source_customers
""")

print("Tabla silver_customers (estado inicial):")
spark.table("silver_customers").orderBy("customer_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Generar eventos CDC (batch 1)
# MAGIC
# MAGIC Simulamos un batch de eventos CDC con tres tipos de operacion:
# MAGIC - **INSERT**: cliente nuevo
# MAGIC - **UPDATE**: cliente existente que cambio de ciudad
# MAGIC - **DELETE**: cliente que se dio de baja

# COMMAND ----------

# Batch 1 de CDC: un insert, dos updates, un delete
cdc_batch_1 = spark.createDataFrame([
    (10, "Elena Suarez", "Bahia Blanca", "2025-07-01", "INSERT",
     datetime(2025, 7, 1, 10, 0, 0)),
    (2, "Santiago Perez", "Buenos Aires", "2024-02-20", "UPDATE",
     datetime(2025, 7, 1, 10, 5, 0)),   # se mudo de Montevideo a Buenos Aires
    (4, "Mateo Fernandez", "Tucuman", "2024-04-05", "UPDATE",
     datetime(2025, 7, 1, 10, 10, 0)),   # se mudo de Rosario a Tucuman
    (8, "Thiago Gonzalez", "Tucuman", "2024-08-30", "DELETE",
     datetime(2025, 7, 1, 10, 15, 0)),   # se dio de baja
], ["customer_id", "name", "city", "registered_at", "_operation", "_event_timestamp"])

cdc_batch_1.write.format("delta").mode("overwrite").saveAsTable("cdc_customers_staging")

print("Batch 1 de CDC:")
spark.table("cdc_customers_staging").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Aplicar CDC con MERGE INTO
# MAGIC
# MAGIC La clave del MERGE es el `ROW_NUMBER()` para deduplicar: si un mismo cliente tiene
# MAGIC multiples eventos en el mismo batch, nos quedamos solo con el ultimo.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_customers AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY customer_id
# MAGIC                 ORDER BY _event_timestamp DESC
# MAGIC             ) AS rn
# MAGIC         FROM cdc_customers_staging
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED AND source._operation = 'DELETE' THEN DELETE
# MAGIC WHEN MATCHED AND source._operation = 'UPDATE' THEN UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.city = source.city,
# MAGIC     target.registered_at = source.registered_at
# MAGIC WHEN NOT MATCHED AND source._operation != 'DELETE' THEN INSERT (
# MAGIC     customer_id, name, city, registered_at
# MAGIC ) VALUES (
# MAGIC     source.customer_id, source.name, source.city, source.registered_at
# MAGIC )

# COMMAND ----------

print("Silver despues del MERGE (batch 1):")
print("- Cliente 10 (Elena): INSERT")
print("- Cliente 2 (Santiago): UPDATE ciudad -> Buenos Aires")
print("- Cliente 4 (Mateo): UPDATE ciudad -> Tucuman")
print("- Cliente 8 (Thiago): DELETE")
print()
spark.table("silver_customers").orderBy("customer_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Segundo batch de CDC -- mas cambios

# COMMAND ----------

# Batch 2: update con multiples eventos para el mismo cliente
# (probamos la deduplicacion con ROW_NUMBER)
cdc_batch_2 = spark.createDataFrame([
    (10, "Elena Suarez", "Mar del Plata", "2025-07-01", "UPDATE",
     datetime(2025, 7, 2, 8, 0, 0)),     # primer update
    (10, "Elena Suarez", "Bariloche", "2025-07-01", "UPDATE",
     datetime(2025, 7, 2, 9, 0, 0)),     # segundo update (este gana por timestamp)
    (11, "Carolina Torres", "Paysandu", "2025-07-02", "INSERT",
     datetime(2025, 7, 2, 10, 0, 0)),
    (1, "Luciana Gomez", "Buenos Aires", "2024-01-15", "DELETE",
     datetime(2025, 7, 2, 11, 0, 0)),
], ["customer_id", "name", "city", "registered_at", "_operation", "_event_timestamp"])

cdc_batch_2.write.format("delta").mode("overwrite").saveAsTable("cdc_customers_staging")

print("Batch 2 de CDC (notar: cliente 10 tiene 2 updates, gana el ultimo por timestamp):")
spark.table("cdc_customers_staging").orderBy("_event_timestamp").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicar batch 2 con el mismo MERGE
# MAGIC MERGE INTO silver_customers AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY customer_id
# MAGIC                 ORDER BY _event_timestamp DESC
# MAGIC             ) AS rn
# MAGIC         FROM cdc_customers_staging
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED AND source._operation = 'DELETE' THEN DELETE
# MAGIC WHEN MATCHED AND source._operation = 'UPDATE' THEN UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.city = source.city,
# MAGIC     target.registered_at = source.registered_at
# MAGIC WHEN NOT MATCHED AND source._operation != 'DELETE' THEN INSERT (
# MAGIC     customer_id, name, city, registered_at
# MAGIC ) VALUES (
# MAGIC     source.customer_id, source.name, source.city, source.registered_at
# MAGIC )

# COMMAND ----------

print("Silver despues del MERGE (batch 2):")
print("- Cliente 10 (Elena): UPDATE -> Bariloche (el segundo update gano)")
print("- Cliente 11 (Carolina): INSERT")
print("- Cliente 1 (Luciana): DELETE")
print()
spark.table("silver_customers").orderBy("customer_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Compactor (OPTIMIZE)
# MAGIC
# MAGIC Cuando un pipeline de streaming o micro-batches escribe muchas veces, genera
# MAGIC **archivos chicos** (small files). El rendimiento de lectura se degrada porque el motor
# MAGIC tiene que abrir miles de archivos.
# MAGIC
# MAGIC `OPTIMIZE` consolida esos archivos en archivos mas grandes. Es un patron de
# MAGIC **mantenimiento** que tendrias que correr periodicamente en cualquier tabla con
# MAGIC escritura frecuente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Simular micro-batches (muchos archivos chicos)

# COMMAND ----------

# Escribir 20 micro-batches de 5 filas cada uno para generar archivos chicos
spark.sql("DROP TABLE IF EXISTS bronze_events_microbatch")

base_ts = datetime(2025, 8, 1, 0, 0, 0)

for batch_num in range(20):
    batch_data = []
    for j in range(5):
        event_id = batch_num * 5 + j + 1
        batch_data.append((
            event_id,
            random.choice(["click", "view", "purchase", "scroll"]),
            random.choice([1, 2, 3, 4, 5]),
            (base_ts + timedelta(minutes=batch_num * 10 + j)).strftime("%Y-%m-%d %H:%M:%S"),
        ))

    batch_df = (
        spark.createDataFrame(batch_data, ["event_id", "event_type", "user_id", "event_time"])
        .withColumn("event_time", F.to_timestamp("event_time"))
    )

    batch_df.write.format("delta").mode("append").saveAsTable("bronze_events_microbatch")

total = spark.table("bronze_events_microbatch").count()
print(f"Total de eventos: {total} (escritos en 20 micro-batches)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Contar archivos ANTES de OPTIMIZE

# COMMAND ----------

# Ver los detalles de la tabla
detail_before = spark.sql("DESCRIBE DETAIL bronze_events_microbatch").select("numFiles", "sizeInBytes")
detail_before.show(truncate=False)

num_files_before = detail_before.collect()[0]["numFiles"]
print(f"Archivos ANTES de OPTIMIZE: {num_files_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Ejecutar OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compactar toda la tabla
# MAGIC OPTIMIZE bronze_events_microbatch

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Contar archivos DESPUES de OPTIMIZE

# COMMAND ----------

detail_after = spark.sql("DESCRIBE DETAIL bronze_events_microbatch").select("numFiles", "sizeInBytes")
detail_after.show(truncate=False)

num_files_after = detail_after.collect()[0]["numFiles"]
print(f"Archivos ANTES:   {num_files_before}")
print(f"Archivos DESPUES: {num_files_after}")
print(f"Reduccion: {num_files_before} -> {num_files_after} archivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 OPTIMIZE con predicado (compactar solo una particion)
# MAGIC
# MAGIC En produccion, si la tabla es grande, no compactas todo: compactas solo
# MAGIC la particion que recibio escrituras recientes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- En produccion, si la tabla esta particionada por fecha, compactarias asi:
# MAGIC -- OPTIMIZE mi_tabla WHERE event_date = current_date() - INTERVAL 1 DAY
# MAGIC --
# MAGIC -- Nuestra tabla no esta particionada, asi que compactamos todo
# MAGIC -- (en tablas chicas como esta, no hay diferencia)
# MAGIC OPTIMIZE bronze_events_microbatch

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Limpieza
# MAGIC
# MAGIC Borramos todo lo que creamos para dejar el workspace limpio.

# COMMAND ----------

tables_to_drop = [
    "source_customers",
    "source_orders",
    "bronze_customers_full",
    "bronze_orders_inc",
    "watermark_control",
    "silver_customers",
    "cdc_customers_staging",
    "bronze_events_microbatch",
]

for table in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table}")
    print(f"Tabla {table} eliminada")

spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA}")
print(f"\nSchema {SCHEMA} eliminado")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumen
# MAGIC
# MAGIC | Patron | Complejidad | Cuando usarlo |
# MAGIC |--------|-------------|---------------|
# MAGIC | **Full Loader** | Baja | Tablas chicas de referencia, fuentes sin columna de control |
# MAGIC | **Incremental Loader** | Media | Tablas transaccionales con `updated_at`, batch diario/horario |
# MAGIC | **CDC (MERGE INTO)** | Alta | Fuentes con updates y deletes, baja latencia |
# MAGIC | **Compactor (OPTIMIZE)** | Baja | Post-streaming, micro-batches, mantenimiento periodico |
# MAGIC
# MAGIC Para mas detalle sobre los 8 patrones (incluyendo Passthrough Replicator, Transformation Replicator,
# MAGIC Readiness Marker y External Trigger), lee el
# MAGIC [blog post completo](https://mauroloprete.github.io/mauroloprete/blog/posts/data-engineering-design-patterns/).
