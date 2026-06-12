# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Setup — Datos de ejemplo para el agente RAG
# MAGIC
# MAGIC Notebook del meetup [De YAML a produccion: Deploy de AI Agents con DABs](https://mauroloprete.github.io/mauroloprete/).
# MAGIC
# MAGIC Este notebook crea la tabla de documentos que el agente usara para responder preguntas.
# MAGIC Simula un export de Confluence con documentacion de soporte tecnico.
# MAGIC
# MAGIC > Requiere un workspace con Unity Catalog y Model Serving habilitado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuracion

# COMMAND ----------

# Parametros del bundle (inyectados via base_parameters)
dbutils.widgets.text("catalog", "dev_bronze")
dbutils.widgets.text("schema", "labs")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear schema si no existe

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
print(f"Schema {FULL_SCHEMA} listo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear tabla de documentos de soporte

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

docs = [
    {
        "doc_id": "DOC-001",
        "title": "Como resetear la contrasena",
        "content": "Para resetear tu contrasena, anda a Configuracion > Seguridad > Cambiar contrasena. "
                   "Si no podes acceder, contacta al admin por el canal #soporte de Slack. "
                   "El link de reset expira en 24 horas. No compartas el link con nadie.",
        "source": "confluence/soporte/accesos",
        "updated_at": datetime(2026, 5, 15)
    },
    {
        "doc_id": "DOC-002",
        "title": "Configurar VPN corporativa",
        "content": "Descargar el cliente VPN desde el portal de IT (portal.internal/vpn). "
                   "Credenciales: mismas que el Active Directory. "
                   "Si da error de certificado, actualizar el cliente a la version 4.2+. "
                   "La VPN es obligatoria para acceder a recursos internos desde fuera de la oficina.",
        "source": "confluence/soporte/redes",
        "updated_at": datetime(2026, 4, 20)
    },
    {
        "doc_id": "DOC-003",
        "title": "Solicitar acceso a Databricks",
        "content": "Levantar un ticket en ServiceNow con categoria 'Acceso a plataforma'. "
                   "Incluir: nombre completo, equipo, proyecto, y nivel de acceso requerido (viewer/editor/admin). "
                   "El acceso se aprueba por el team lead y se provisiona en 24-48 horas habiles. "
                   "Para acceso temporal (menos de 30 dias), usar el formulario express.",
        "source": "confluence/soporte/plataforma",
        "updated_at": datetime(2026, 6, 1)
    },
    {
        "doc_id": "DOC-004",
        "title": "Troubleshooting: job fallido en Databricks",
        "content": "Pasos para diagnosticar un job fallido: "
                   "1. Revisar los logs en la UI de Workflows > click en el run fallido. "
                   "2. Buscar el error en la pestaña 'Output' de la task. "
                   "3. Si es OOM, aumentar el cluster o usar un instance type mas grande. "
                   "4. Si es timeout, revisar si hay un lock en la tabla Delta. "
                   "5. Para errores de permisos, verificar que el service principal tiene acceso al catalog.",
        "source": "confluence/soporte/databricks",
        "updated_at": datetime(2026, 5, 28)
    },
    {
        "doc_id": "DOC-005",
        "title": "Politica de backups y disaster recovery",
        "content": "Los backups de Delta Tables se hacen automaticamente via Time Travel (30 dias). "
                   "Para retener mas tiempo, usar DEEP CLONE a una tabla de archivo. "
                   "El RPO objetivo es de 1 hora para tablas criticas. "
                   "En caso de desastre, contactar al equipo de infraestructura por el canal #infra-urgencias.",
        "source": "confluence/soporte/infra",
        "updated_at": datetime(2026, 3, 10)
    },
]

df = spark.createDataFrame(docs)
df.write.mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.soporte_docs")

display(spark.table(f"{FULL_SCHEMA}.soporte_docs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar

# COMMAND ----------

count = spark.table(f"{FULL_SCHEMA}.soporte_docs").count()
print(f"Tabla {FULL_SCHEMA}.soporte_docs creada con {count} documentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC | Recurso | Estado |
# MAGIC |---------|--------|
# MAGIC | Schema `${CATALOG}.${SCHEMA}` | Creado |
# MAGIC | Tabla `soporte_docs` | 5 documentos de ejemplo |
# MAGIC
# MAGIC Siguiente paso: ejecutar `01_agent.py` para registrar el agente en MLflow.
