# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Tips — Laboratorio practico
# MAGIC
# MAGIC Notebook del blog post [Databricks Tips #2: 7 cosas de Delta Lake que ojala me hubieran dicho antes](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-01-delta-lake/).
# MAGIC
# MAGIC Aca vas a probar en primera persona: Liquid Clustering, OPTIMIZE con predicados, VACUUM, Time Travel, Change Data Feed y mas.
# MAGIC
# MAGIC > Ejecutable en **Databricks Free Edition** con Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 0. Setup — Crear datos de ejemplo
# MAGIC
# MAGIC Vamos a crear una tabla de transacciones para usar en todo el lab.
# MAGIC Usamos `workspace.default` que viene disponible en Free Edition con Unity Catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Limpiar si ya existe de una corrida anterior
# MAGIC DROP TABLE IF EXISTS workspace.default.lab_transacciones;
# MAGIC DROP TABLE IF EXISTS workspace.default.lab_transacciones_zorder;
# MAGIC DROP TABLE IF EXISTS workspace.default.lab_transacciones_debug;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla principal: transacciones con Liquid Clustering
# MAGIC CREATE TABLE workspace.default.lab_transacciones (
# MAGIC   tx_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   fecha DATE,
# MAGIC   usuario_id INT,
# MAGIC   monto DECIMAL(10, 2),
# MAGIC   categoria STRING,
# MAGIC   estado STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (fecha, usuario_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insertar datos de ejemplo: transacciones de los ultimos dias
# MAGIC INSERT INTO workspace.default.lab_transacciones (fecha, usuario_id, monto, categoria, estado)
# MAGIC VALUES
# MAGIC   (current_date() - INTERVAL 5 DAYS, 1, 150.00, 'electronica', 'completada'),
# MAGIC   (current_date() - INTERVAL 5 DAYS, 2, 89.50, 'ropa', 'completada'),
# MAGIC   (current_date() - INTERVAL 5 DAYS, 3, 230.00, 'electronica', 'completada'),
# MAGIC   (current_date() - INTERVAL 4 DAYS, 1, 45.00, 'comida', 'completada'),
# MAGIC   (current_date() - INTERVAL 4 DAYS, 4, 320.00, 'electronica', 'pendiente'),
# MAGIC   (current_date() - INTERVAL 4 DAYS, 2, 67.80, 'comida', 'completada'),
# MAGIC   (current_date() - INTERVAL 3 DAYS, 5, 199.99, 'ropa', 'completada'),
# MAGIC   (current_date() - INTERVAL 3 DAYS, 3, 55.00, 'comida', 'cancelada'),
# MAGIC   (current_date() - INTERVAL 3 DAYS, 1, 410.00, 'electronica', 'completada'),
# MAGIC   (current_date() - INTERVAL 2 DAYS, 6, 78.50, 'ropa', 'pendiente'),
# MAGIC   (current_date() - INTERVAL 2 DAYS, 2, 125.00, 'electronica', 'completada'),
# MAGIC   (current_date() - INTERVAL 2 DAYS, 4, 93.20, 'comida', 'completada'),
# MAGIC   (current_date() - INTERVAL 1 DAY,  5, 560.00, 'electronica', 'completada'),
# MAGIC   (current_date() - INTERVAL 1 DAY,  1, 34.90, 'comida', 'completada'),
# MAGIC   (current_date() - INTERVAL 1 DAY,  3, 210.00, 'ropa', 'pendiente'),
# MAGIC   (current_date(), 6, 145.00, 'electronica', 'pendiente'),
# MAGIC   (current_date(), 2, 88.00, 'comida', 'completada'),
# MAGIC   (current_date(), 4, 375.00, 'ropa', 'completada');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que los datos estan bien
# MAGIC SELECT * FROM workspace.default.lab_transacciones ORDER BY fecha, usuario_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Liquid Clustering vs Z-ORDER
# MAGIC
# MAGIC **Liquid Clustering** (DBR 13.3+) reemplaza a Z-ORDER. Las diferencias clave:
# MAGIC
# MAGIC | | Z-ORDER | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | Se define al crear la tabla | No | Si, con `CLUSTER BY` |
# MAGIC | Se ejecuta automaticamente | No, hay que correr `OPTIMIZE ... ZORDER BY` | Si, de forma incremental |
# MAGIC | Se pueden cambiar las columnas | Hay que reescribir | `ALTER TABLE ... CLUSTER BY (nuevas_cols)` |
# MAGIC | Adaptativo | No | Si, se adapta a patrones de consulta |
# MAGIC
# MAGIC Nuestra tabla `lab_transacciones` ya tiene Liquid Clustering (la creamos con `CLUSTER BY`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que Liquid Clustering esta activo
# MAGIC DESCRIBE DETAIL workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cambiar las columnas de clustering sin reescribir la tabla
# MAGIC -- (esto con Z-ORDER no se puede hacer)
# MAGIC ALTER TABLE workspace.default.lab_transacciones
# MAGIC CLUSTER BY (fecha, categoria);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que cambio
# MAGIC DESCRIBE DETAIL workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Volver al clustering original
# MAGIC ALTER TABLE workspace.default.lab_transacciones
# MAGIC CLUSTER BY (fecha, usuario_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Para comparar: crear una tabla SIN liquid clustering (forma clasica)
# MAGIC -- Particionamos por fecha para poder usar OPTIMIZE WHERE despues
# MAGIC CREATE TABLE workspace.default.lab_transacciones_zorder (
# MAGIC   tx_id BIGINT,
# MAGIC   usuario_id INT,
# MAGIC   monto DECIMAL(10, 2),
# MAGIC   categoria STRING,
# MAGIC   estado STRING,
# MAGIC   fecha DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (fecha);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.default.lab_transacciones_zorder
# MAGIC SELECT tx_id, usuario_id, monto, categoria, estado, fecha
# MAGIC FROM workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Con Z-ORDER, tenes que correr esto manualmente cada vez
# MAGIC -- Z-ORDER se aplica sobre columnas NO particionadas
# MAGIC OPTIMIZE workspace.default.lab_transacciones_zorder
# MAGIC ZORDER BY (usuario_id);

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusion**: si estas en DBR 13.3+, usa Liquid Clustering. Es menos laburo, mas eficiente, y se adapta solo.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. OPTIMIZE con predicados
# MAGIC
# MAGIC Mucha gente corre `OPTIMIZE` en toda la tabla como cron job diario. El problema:
# MAGIC sin predicado reescribe **toda** la tabla. En tablas de TB, eso son horas de compute.
# MAGIC
# MAGIC Con un predicado `WHERE`, solo reescribe los archivos que matchean.
# MAGIC
# MAGIC **Importante**: Liquid Clustering + `OPTIMIZE` ya es incremental de por si
# MAGIC (solo toca archivos no clusterizados). No necesitas `WHERE` con Liquid Clustering.
# MAGIC El `WHERE` es util para tablas **sin** Liquid Clustering (ej: con Z-ORDER).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Con Liquid Clustering: OPTIMIZE corre incremental, sin WHERE
# MAGIC -- Solo toca los archivos que todavia no estan bien clusterizados
# MAGIC OPTIMIZE workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- En tablas SIN Liquid Clustering, usas WHERE para ser eficiente
# MAGIC -- Esto solo optimiza los archivos que matchean el predicado
# MAGIC OPTIMIZE workspace.default.lab_transacciones_zorder
# MAGIC WHERE fecha = current_date() - INTERVAL 1 DAY;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revisar el historial de la tabla con LC
# MAGIC DESCRIBE HISTORY workspace.default.lab_transacciones LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Resumen de OPTIMIZE**:
# MAGIC - Con **Liquid Clustering**: correr `OPTIMIZE` sin `WHERE`. Ya es incremental.
# MAGIC - Sin Liquid Clustering: usar `OPTIMIZE ... WHERE` para no reescribir toda la tabla.
# MAGIC - Es seguro correr OPTIMIZE seguido: no reescribe lo que ya esta bien.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. VACUUM — limpieza de archivos y logs
# MAGIC
# MAGIC `VACUUM` borra archivos de datos viejos que ya no son parte de la version activa de la tabla.
# MAGIC El default de retencion es 7 dias.
# MAGIC
# MAGIC **Clave**: VACUUM y el log de Delta son cosas distintas:
# MAGIC
# MAGIC | Concepto | Que limpia | Propiedad |
# MAGIC |---|---|---|
# MAGIC | `VACUUM` | Archivos Parquet huerfanos | `delta.deletedFileRetentionDuration` (default 7 dias) |
# MAGIC | Log retention | Archivos JSON/Parquet del transaction log | `delta.logRetentionDuration` (default 30 dias) |
# MAGIC
# MAGIC VACUUM **no** borra los archivos de log. Para eso se usa `delta.logRetentionDuration`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver los detalles de la tabla (incluyendo propiedades de retencion)
# MAGIC DESCRIBE DETAIL workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Correr VACUUM con retencion de 168 horas (7 dias, el default)
# MAGIC -- En un lab recien creado probablemente no borre nada, pero asi se usa
# MAGIC VACUUM workspace.default.lab_transacciones RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DRY RUN: ver que archivos borraria sin borrar nada
# MAGIC VACUUM workspace.default.lab_transacciones RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Configurar la retencion del log (transaction log, NO los datos)
# MAGIC -- Default es 30 dias. En produccion, ajusta segun tu ventana de time travel.
# MAGIC ALTER TABLE workspace.default.lab_transacciones
# MAGIC SET TBLPROPERTIES (
# MAGIC   delta.logRetentionDuration = 'interval 30 days',
# MAGIC   delta.deletedFileRetentionDuration = 'interval 7 days'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar las propiedades
# MAGIC SHOW TBLPROPERTIES workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver el historial completo de operaciones
# MAGIC -- Aca se ve cada commit al transaction log
# MAGIC DESCRIBE HISTORY workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %md
# MAGIC **En produccion**: automatiza VACUUM con un job semanal. Nunca uses retencion
# MAGIC menor a la ventana de time travel que necesites (si necesitas 7 dias de time travel,
# MAGIC VACUUM debe retener al menos 7 dias).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Time Travel
# MAGIC
# MAGIC Delta Lake guarda el historial completo de cambios. Podes:
# MAGIC - Consultar versiones anteriores (`VERSION AS OF`, `TIMESTAMP AS OF`)
# MAGIC - Restaurar la tabla a un estado anterior (`RESTORE`)
# MAGIC - Comparar versiones para validar pipelines
# MAGIC - Hacer clones livianos para debugging

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver todas las versiones disponibles
# MAGIC DESCRIBE HISTORY workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consultar la version 0 (el estado inicial, antes de los INSERTs)
# MAGIC -- Nota: version 0 es el CREATE TABLE, version 1 es el primer INSERT
# MAGIC SELECT * FROM workspace.default.lab_transacciones VERSION AS OF 1
# MAGIC ORDER BY fecha, usuario_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hacer un cambio para tener algo que restaurar
# MAGIC UPDATE workspace.default.lab_transacciones
# MAGIC SET estado = 'reembolsada'
# MAGIC WHERE estado = 'cancelada';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar el cambio
# MAGIC SELECT * FROM workspace.default.lab_transacciones
# MAGIC WHERE estado = 'reembolsada';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver que version se creo con el UPDATE
# MAGIC DESCRIBE HISTORY workspace.default.lab_transacciones LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RESTORE: volver al estado antes del UPDATE
# MAGIC -- Usa el numero de version anterior al UPDATE (revisalo en el DESCRIBE HISTORY de arriba)
# MAGIC -- Aca asumimos que el UPDATE fue la version mas reciente
# MAGIC RESTORE TABLE workspace.default.lab_transacciones TO VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar: ya no deberia haber filas con estado 'reembolsada'
# MAGIC SELECT estado, count(*) as cantidad
# MAGIC FROM workspace.default.lab_transacciones
# MAGIC GROUP BY estado;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHALLOW CLONE: crear una copia liviana para debugging
# MAGIC -- No copia los datos, solo referencia los archivos originales
# MAGIC CREATE OR REPLACE TABLE workspace.default.lab_transacciones_debug
# MAGIC SHALLOW CLONE workspace.default.lab_transacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El clone tiene los mismos datos pero es independiente
# MAGIC SELECT count(*) as filas_en_clone FROM workspace.default.lab_transacciones_debug;

# COMMAND ----------

# MAGIC %md
# MAGIC **Tip**: `SHALLOW CLONE` es muy util para investigar datos sin tocar la tabla original.
# MAGIC Podrias, por ejemplo, clonar una version especifica:
# MAGIC ```sql
# MAGIC CREATE TABLE debug_v5 SHALLOW CLONE mi_tabla VERSION AS OF 5;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Change Data Feed (CDF)
# MAGIC
# MAGIC Si tenes pipelines downstream que necesitan saber **que cambio** en una tabla,
# MAGIC no necesitas comparar snapshots. Activa CDF y Delta Lake trackea cada cambio
# MAGIC con metadata extra: `_change_type`, `_commit_version`, `_commit_timestamp`.
# MAGIC
# MAGIC Los tipos de cambio son:
# MAGIC - `insert` — fila nueva
# MAGIC - `update_preimage` — valor anterior al UPDATE
# MAGIC - `update_postimage` — valor nuevo despues del UPDATE
# MAGIC - `delete` — fila borrada

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Activar Change Data Feed en la tabla
# MAGIC ALTER TABLE workspace.default.lab_transacciones
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# Guardamos la version actual — la necesitamos para leer los cambios despues
cdf_start = spark.sql("""
    DESCRIBE HISTORY workspace.default.lab_transacciones LIMIT 1
""").select("version").first()[0]
print(f"CDF activado en version: {cdf_start}")
print("Los cambios a partir de la siguiente version se van a trackear.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hacer un INSERT para generar cambios trackeados por CDF
# MAGIC INSERT INTO workspace.default.lab_transacciones (fecha, usuario_id, monto, categoria, estado)
# MAGIC VALUES
# MAGIC   (current_date(), 7, 999.99, 'electronica', 'completada'),
# MAGIC   (current_date(), 8, 50.00, 'comida', 'pendiente');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hacer un UPDATE para ver los preimage/postimage
# MAGIC UPDATE workspace.default.lab_transacciones
# MAGIC SET estado = 'cancelada', monto = 0.00
# MAGIC WHERE usuario_id = 8 AND estado = 'pendiente';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hacer un DELETE
# MAGIC DELETE FROM workspace.default.lab_transacciones
# MAGIC WHERE usuario_id = 7 AND monto = 999.99;

# COMMAND ----------

# Leer el Change Data Feed usando la version que capturamos
# Usamos cdf_start + 1 porque los cambios empiezan DESPUES de activar CDF
df_cdf = spark.sql(f"""
    SELECT *
    FROM table_changes('workspace.default.lab_transacciones', {cdf_start + 1})
    ORDER BY _commit_version, _change_type
""")
df_cdf.show(20, truncate=50)

# COMMAND ----------

# Tambien se puede leer con la API de DataFrameReader
df_changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", cdf_start + 1)
    .table("workspace.default.lab_transacciones")
)

(df_changes
    .select("tx_id", "usuario_id", "monto", "estado", "_change_type", "_commit_version")
    .orderBy("_commit_version", "_change_type")
    .show(20, truncate=50))

# COMMAND ----------

# Filtrar solo los updates (ver el antes y despues)
(df_changes
    .filter("_change_type IN ('update_preimage', 'update_postimage')")
    .select("tx_id", "usuario_id", "monto", "estado", "_change_type", "_commit_version")
    .orderBy("_commit_version", "tx_id", "_change_type")
    .show(truncate=50))

# COMMAND ----------

# MAGIC %md
# MAGIC **CDF es la base para hacer CDC eficiente** sin herramientas externas.
# MAGIC En vez de comparar snapshots completos, lees solo lo que cambio entre versiones.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Cleanup
# MAGIC
# MAGIC Borramos todas las tablas que creamos en el lab.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.default.lab_transacciones;
# MAGIC DROP TABLE IF EXISTS workspace.default.lab_transacciones_zorder;
# MAGIC DROP TABLE IF EXISTS workspace.default.lab_transacciones_debug;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumen
# MAGIC
# MAGIC | Concepto | Que aprendimos |
# MAGIC |---|---|
# MAGIC | Liquid Clustering | `CLUSTER BY` al crear la tabla, se optimiza solo, se puede cambiar sin reescribir |
# MAGIC | OPTIMIZE con predicados | Siempre usar `WHERE` en tablas grandes para no reescribir todo |
# MAGIC | VACUUM | Limpia archivos de datos, no el transaction log. Nunca con retencion < ventana de time travel |
# MAGIC | Log retention | `delta.logRetentionDuration` controla el log, es independiente de VACUUM |
# MAGIC | Time Travel | `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE`, `SHALLOW CLONE` |
# MAGIC | Change Data Feed | `delta.enableChangeDataFeed = true` + `table_changes()` para CDC eficiente |
# MAGIC
# MAGIC **Blog post completo**: [Databricks Tips #2: 7 cosas de Delta Lake](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-01-delta-lake/)
