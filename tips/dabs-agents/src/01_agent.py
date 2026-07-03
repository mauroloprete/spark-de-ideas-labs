# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Agente RAG — registro en MLflow + Unity Catalog
# MAGIC
# MAGIC Notebook del meetup [De YAML a produccion: Deploy de AI Agents con DABs](https://mauroloprete.github.io/mauroloprete/).
# MAGIC
# MAGIC En este lab vas a:
# MAGIC 1. Crear un agente RAG como MLflow `PythonModel`
# MAGIC 2. Registrar el modelo en Unity Catalog
# MAGIC 3. Verificar que queda listo para servir con DABs
# MAGIC
# MAGIC > Requiere un workspace con Unity Catalog y Foundation Model APIs habilitados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuracion

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_bronze")
dbutils.widgets.text("schema", "labs")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
MODEL_NAME = f"{FULL_SCHEMA}.soporte_bot"

print(f"Modelo: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Definir el agente RAG

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
from mlflow.models.resources import DatabricksServingEndpoint
import pandas as pd

# Configurar el experiment del lab (usa el path del usuario actual)
username = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{username}/dabs-agents-lab")

# COMMAND ----------

class SoporteBot(mlflow.pyfunc.PythonModel):
    """
    Agente RAG simplificado para soporte tecnico.
    Usa mlflow.deployments para llamar a Foundation Model APIs (auth automatica en Model Serving).
    Compatible con Model Serving (no usa PySpark ni Databricks SDK).
    """

    # Documentos de soporte
    DOCS = [
        {"doc_id": "DOC-001", "title": "Como resetear la contrasena",
         "content": "Para resetear tu contrasena, anda a Configuracion > Seguridad > Cambiar contrasena. Si no podes acceder, contacta al admin."},
        {"doc_id": "DOC-002", "title": "Configurar VPN corporativa",
         "content": "Descargar el cliente VPN desde el portal de IT (portal.internal/vpn). Credenciales: mismas que el Active Directory."},
        {"doc_id": "DOC-003", "title": "Solicitar acceso a Databricks",
         "content": "Levantar un ticket en ServiceNow con categoria Acceso a plataforma. Incluir nombre, equipo, proyecto, nivel de acceso."},
    ]

    def load_context(self, context):
        """Se ejecuta una vez cuando el modelo se carga en el endpoint."""
        from mlflow.deployments import get_deploy_client
        self._deploy_client = get_deploy_client("databricks")
        self._docs = list(self.DOCS)

    def _search_docs(self, question: str) -> str:
        """Busca documentos relevantes por keywords."""
        import re
        keywords = re.findall(r'\w+', question.lower())

        scored = []
        for doc in self._docs:
            text = (doc["title"] + " " + doc["content"]).lower()
            score = sum(1 for kw in keywords if kw in text)
            if score > 0:
                scored.append((score, doc["title"], doc["content"]))

        scored.sort(reverse=True)
        top_docs = scored[:3]

        if not top_docs:
            return "No se encontraron documentos relevantes."

        return "\n\n".join(
            [f"## {title}\n{content}" for _, title, content in top_docs]
        )

    def predict(self, context, model_input, params=None):
        """Recibe una pregunta y devuelve la respuesta del agente."""
        if isinstance(model_input, pd.DataFrame):
            question = model_input["question"].iloc[0]
        else:
            question = str(model_input)

        # 1. Buscar documentos relevantes
        doc_context = self._search_docs(question)

        # 2. Generar respuesta con Foundation Model API via mlflow.deployments
        prompt = f"""Sos un asistente de soporte tecnico. Responde la pregunta usando SOLO la informacion
de los documentos proporcionados. Si no encontras la respuesta, deci que no tenes informacion.

DOCUMENTOS:
{doc_context}

PREGUNTA: {question}

RESPUESTA:"""

        response = self._deploy_client.predict(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            inputs={
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,
            },
        )

        return response["choices"][0]["message"]["content"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Registrar el modelo en MLflow + Unity Catalog

# COMMAND ----------

# Signature del modelo
input_example = pd.DataFrame({"question": ["Como reseteo mi contrasena?"]})
output_example = "Para resetear tu contrasena, anda a Configuracion > Seguridad > Cambiar contrasena."

signature = infer_signature(input_example, output_example)

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="soporte-bot-v1") as run:
    mlflow.log_param("retrieval", "keyword-search")
    mlflow.log_param("llm", "meta-llama-3.3-70b-instruct")
    mlflow.log_param("docs_table", f"{FULL_SCHEMA}.soporte_docs")

    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=SoporteBot(),
        registered_model_name=MODEL_NAME,
        signature=signature,
        input_example=input_example,
        pip_requirements=[
            "mlflow>=2.18.0",
            "pandas",
        ],
        resources=[
            DatabricksServingEndpoint(endpoint_name="databricks-meta-llama-3-3-70b-instruct"),
        ],
    )

    print(f"Run ID:      {run.info.run_id}")
    print(f"Model URI:   {model_info.model_uri}")
    print(f"Registrado:  {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar el modelo en Unity Catalog

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
model_versions = client.search_model_versions(f"name='{MODEL_NAME}'")

for v in model_versions:
    print(f"Version {v.version} | Status: {v.status} | Run: {v.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC | Paso | Que hicimos |
# MAGIC |------|-------------|
# MAGIC | **Agente RAG** | `SoporteBot` como `mlflow.pyfunc.PythonModel` |
# MAGIC | **Retrieval** | Busqueda por keywords en tabla Delta (en prod usarias Vector Search) |
# MAGIC | **LLM** | Foundation Model API (Meta Llama 3.3 70B) |
# MAGIC | **Registro** | Modelo en Unity Catalog via `mlflow.pyfunc.log_model` |
# MAGIC
# MAGIC El modelo queda listo para servir. DABs se encarga del endpoint con:
# MAGIC ```yaml
# MAGIC model_serving_endpoints:
# MAGIC   agent_endpoint:
# MAGIC     config:
# MAGIC       served_entities:
# MAGIC         - entity_name: ${var.catalog}.${var.schema}.soporte_bot
# MAGIC           entity_version: "1"
# MAGIC ```
