# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Refresh del indice de documentos
# MAGIC
# MAGIC Notebook del meetup [De YAML a produccion: Deploy de AI Agents con DABs](https://mauroloprete.github.io/mauroloprete/).
# MAGIC
# MAGIC Este notebook simula el job de refresh que mantiene actualizada la base de conocimiento del agente.
# MAGIC En produccion, esto se ejecutaria como un job programado via DABs.
# MAGIC
# MAGIC > Requiere Unity Catalog habilitado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuracion

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_bronze")
dbutils.widgets.text("schema", "labs")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

print(f"Schema: {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar datos actuales

# COMMAND ----------

current_count = spark.table(f"{FULL_SCHEMA}.soporte_docs").count()
print(f"Documentos actuales: {current_count}")

display(
    spark.sql(f"""
        SELECT doc_id, title, source, updated_at
        FROM {FULL_SCHEMA}.soporte_docs
        ORDER BY updated_at DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simular llegada de documentos nuevos

# COMMAND ----------

from datetime import datetime

new_docs = [
    {
        "doc_id": "DOC-006",
        "title": "Guia de onboarding para nuevos empleados",
        "content": "Primer dia: activar cuenta de AD con el link del mail de bienvenida. "
                   "Segundo dia: instalar VPN y configurar MFA. "
                   "Tercer dia: solicitar accesos a las plataformas de tu equipo via ServiceNow. "
                   "El buddy asignado te ayuda durante la primera semana.",
        "source": "confluence/soporte/onboarding",
        "updated_at": datetime.now()
    },
]

df_new = spark.createDataFrame(new_docs)
df_new.write.mode("append").saveAsTable(f"{FULL_SCHEMA}.soporte_docs")

new_count = spark.table(f"{FULL_SCHEMA}.soporte_docs").count()
print(f"Documentos despues del refresh: {new_count} (+{new_count - current_count} nuevos)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. En produccion: sync con Vector Search
# MAGIC
# MAGIC En un caso real, despues de actualizar la tabla ejecutarias:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC
# MAGIC vsc = VectorSearchClient()
# MAGIC index = vsc.get_index(
# MAGIC     endpoint_name="vs-endpoint",
# MAGIC     index_name=f"{CATALOG}.{SCHEMA}.soporte_docs_index"
# MAGIC )
# MAGIC
# MAGIC # Sync Delta Table -> Vector Index (incremental)
# MAGIC index.sync()
# MAGIC ```
# MAGIC
# MAGIC Con un **Delta Sync Index**, la sincronizacion es automatica al actualizar la tabla.
# MAGIC DABs programa este job con:
# MAGIC
# MAGIC ```yaml
# MAGIC jobs:
# MAGIC   refresh_index:
# MAGIC     schedule:
# MAGIC       quartz_cron_expression: "0 0 6 * * ?"
# MAGIC       timezone_id: "America/Montevideo"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC | Paso | Que hicimos |
# MAGIC |------|-------------|
# MAGIC | **Verificar** | Conteo de documentos existentes |
# MAGIC | **Ingestar** | Append de documentos nuevos a la tabla Delta |
# MAGIC | **Siguiente** | En produccion, sync automatico con Vector Search |
# MAGIC
# MAGIC Este job lo ejecuta DABs cada dia a las 6 AM (America/Montevideo).
