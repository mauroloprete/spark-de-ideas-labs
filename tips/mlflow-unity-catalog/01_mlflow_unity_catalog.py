# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow + Unity Catalog — Lab
# MAGIC
# MAGIC Notebook del blog post [Databricks Tips #5: MLflow + Unity Catalog](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-04-mlflow-unity-catalog/).
# MAGIC
# MAGIC En este lab vas a recorrer el flujo completo de MLflow integrado con Unity Catalog:
# MAGIC desde crear un experimento hasta registrar un modelo, asignarle un alias y cargarlo para inferencia.
# MAGIC
# MAGIC > Ejecutable en Databricks Free Edition con Unity Catalog habilitado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup
# MAGIC
# MAGIC Importamos las librerías necesarias y configuramos MLflow para que use Unity Catalog como registry.
# MAGIC
# MAGIC La línea clave es `mlflow.set_registry_uri("databricks-uc")`.
# MAGIC Sin esto, MLflow usa el Workspace Registry viejo. Con esto, los modelos se registran
# MAGIC en Unity Catalog y heredan toda su gobernanza: permisos, lineage, auditoría.

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

import pandas as pd
import numpy as np

# Configurar MLflow para usar Databricks como tracking server y Unity Catalog como registry
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

client = MlflowClient()

print(f"MLflow version: {mlflow.__version__}")
print(f"Tracking URI: {mlflow.get_tracking_uri()}")
print(f"Registry URI: {mlflow.get_registry_uri()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear un experimento
# MAGIC
# MAGIC Un **experimento** en MLflow es un contenedor de runs. Cada run es una ejecución
# MAGIC individual donde se loguean parametros, metricas y artefactos (como el modelo).
# MAGIC
# MAGIC Pensalo asi:
# MAGIC - **Experimento** = el problema que querés resolver (ej: "clasificar iris")
# MAGIC - **Run** = un intento particular con ciertos hiperparametros
# MAGIC
# MAGIC Si el experimento ya existe, `set_experiment` lo reutiliza sin crear uno nuevo.

# COMMAND ----------

EXPERIMENT_NAME = "/Users/" + spark.sql("SELECT current_user()").first()[0] + "/lab_mlflow_uc"

mlflow.set_experiment(EXPERIMENT_NAME)
experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)

print(f"Experimento: {experiment.name}")
print(f"Experiment ID: {experiment.experiment_id}")
print(f"Artifact Location: {experiment.artifact_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar datos
# MAGIC
# MAGIC Usamos el dataset Iris, que es simple y viene con sklearn.
# MAGIC El foco del lab es el flujo de MLflow, no la calidad del modelo.

# COMMAND ----------

# Cargar dataset
iris = load_iris()

X = pd.DataFrame(iris.data, columns=iris.feature_names)
y = pd.Series(iris.target, name="target")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"Datos de entrenamiento: {X_train.shape[0]} filas")
print(f"Datos de test: {X_test.shape[0]} filas")
print(f"Features: {list(X.columns)}")
print(f"Clases: {list(iris.target_names)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Entrenar modelo con autolog
# MAGIC
# MAGIC `mlflow.autolog()` es la forma mas directa de capturar todo automaticamente:
# MAGIC parametros, metricas, el modelo serializado, y hasta la firma (input/output schema).
# MAGIC
# MAGIC No hace falta escribir `mlflow.log_param()` ni `mlflow.log_metric()` a mano.
# MAGIC Autolog detecta que estamos usando sklearn y loguea todo.

# COMMAND ----------

# Activar autolog para sklearn
mlflow.autolog()

with mlflow.start_run(run_name="rf_iris_autolog") as run:
    # Entrenar el modelo
    rf = RandomForestClassifier(
        n_estimators=100,
        max_depth=5,
        random_state=42,
    )
    rf.fit(X_train, y_train)

    # Evaluar
    y_pred = rf.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    run_id = run.info.run_id
    print(f"Run ID: {run_id}")
    print(f"Accuracy: {accuracy:.4f}")

# Desactivar autolog para que no interfiera con las celdas siguientes
mlflow.autolog(disable=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Autolog capturó automaticamente:
# MAGIC - **Parametros**: `n_estimators`, `max_depth`, `random_state`, etc.
# MAGIC - **Metricas**: `training_accuracy_score`, etc.
# MAGIC - **Artefactos**: el modelo serializado, requirements, input example
# MAGIC
# MAGIC Podés verlo en la UI de MLflow: hacé click en "Experiments" en la sidebar izquierda.

# COMMAND ----------

# Verificar lo que autolog capturó
logged_run = mlflow.get_run(run_id)

print("--- Parametros logueados ---")
for k, v in sorted(logged_run.data.params.items()):
    print(f"  {k}: {v}")

print("\n--- Metricas logueadas ---")
for k, v in sorted(logged_run.data.metrics.items()):
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Registrar modelo en Unity Catalog
# MAGIC
# MAGIC Aca está la diferencia clave con el Workspace Registry viejo:
# MAGIC
# MAGIC | | Workspace Registry | Unity Catalog |
# MAGIC |---|---|---|
# MAGIC | Nombre del modelo | `mi_modelo` | `catalog.schema.mi_modelo` |
# MAGIC | Gobernanza | Solo por workspace | Centralizada, cross-workspace |
# MAGIC | Permisos | Basicos | GRANT/REVOKE con ACLs |
# MAGIC | Lineage | No | Si, de datos a modelo |
# MAGIC | Stages | None/Staging/Production/Archived | Aliases (flexible) |
# MAGIC
# MAGIC El nombre del modelo en UC sigue el formato de tres niveles: `catalog.schema.model_name`.

# COMMAND ----------

MODEL_NAME = "workspace.default.lab_iris_model"

# Registrar el modelo desde el run que acabamos de hacer
model_uri = f"runs:/{run_id}/model"
registered = mlflow.register_model(model_uri, MODEL_NAME)

print(f"Modelo registrado: {registered.name}")
print(f"Version: {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Aliases en vez de stages
# MAGIC
# MAGIC En el Workspace Registry viejo, un modelo pasaba por stages fijos:
# MAGIC `None` -> `Staging` -> `Production` -> `Archived`.
# MAGIC
# MAGIC El problema: era rigido. ¿Y si querias tener "canary"? ¿O "shadow"? No se podia.
# MAGIC
# MAGIC Unity Catalog reemplazó los stages con **aliases**: etiquetas de texto libre
# MAGIC que vos asignas a una version. Algunos alias comunes:
# MAGIC
# MAGIC - `champion` — el modelo en produccion
# MAGIC - `challenger` — un candidato que estás evaluando
# MAGIC - `baseline` — la version de referencia para comparar
# MAGIC
# MAGIC Un alias apunta a exactamente una version. Si lo reasignas, se mueve.

# COMMAND ----------

# Asignar alias "champion" a la version que acabamos de registrar
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="champion",
    version=registered.version,
)

print(f"Alias 'champion' asignado a version {registered.version}")

# Verificar
version_info = client.get_model_version_by_alias(MODEL_NAME, "champion")
print(f"Version del champion: {version_info.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC Para mover el alias a otra version (por ejemplo, después de un A/B test),
# MAGIC simplemente volvés a llamar `set_registered_model_alias` con la nueva version.
# MAGIC El alias se mueve automaticamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lineage: de datos a modelo
# MAGIC
# MAGIC Una de las ventajas mas importantes de UC es el lineage automatico.
# MAGIC Cuando logueás los datos de entrada con `mlflow.log_input()`, Unity Catalog
# MAGIC sabe exactamente qué datos se usaron para entrenar cada version del modelo.
# MAGIC
# MAGIC Esto se ve en la UI: si vas al modelo en el Catalog Explorer,
# MAGIC podés ver qué tablas alimentaron el entrenamiento.
# MAGIC
# MAGIC Acá lo hacemos con un dataset de MLflow (no una tabla Delta),
# MAGIC pero el concepto es el mismo.

# COMMAND ----------

# Crear un nuevo run que loguee el input dataset explicitamente
with mlflow.start_run(run_name="rf_iris_con_lineage") as run_lineage:
    # Crear dataset de MLflow a partir del DataFrame de pandas
    train_with_target = X_train.copy()
    train_with_target["target"] = y_train.values
    dataset = mlflow.data.from_pandas(train_with_target, targets="target", name="iris_train")

    # Loguear el dataset — esto crea el lineage
    mlflow.log_input(dataset, context="training")

    # Entrenar
    rf2 = RandomForestClassifier(n_estimators=150, max_depth=6, random_state=42)
    rf2.fit(X_train, y_train)

    y_pred2 = rf2.predict(X_test)
    accuracy2 = accuracy_score(y_test, y_pred2)

    # Loguear metricas manualmente (sin autolog)
    mlflow.log_param("n_estimators", 150)
    mlflow.log_param("max_depth", 6)
    mlflow.log_metric("test_accuracy", accuracy2)

    # Loguear el modelo con firma
    signature = mlflow.models.infer_signature(X_train, rf2.predict(X_train))
    mlflow.sklearn.log_model(rf2, "model", signature=signature)

    run_lineage_id = run_lineage.info.run_id
    print(f"Run ID: {run_lineage_id}")
    print(f"Accuracy: {accuracy2:.4f}")

# COMMAND ----------

# Verificar que se logueó el dataset
run_data = mlflow.get_run(run_lineage_id)
inputs = run_data.inputs.dataset_inputs

print(f"Datasets logueados: {len(inputs)}")
for ds_input in inputs:
    print(f"  Nombre: {ds_input.dataset.name}")
    print(f"  Contexto: {ds_input.tags[0].value if ds_input.tags else 'N/A'}")

# COMMAND ----------

# Registrar esta version también y mover el alias champion
model_uri_v2 = f"runs:/{run_lineage_id}/model"
registered_v2 = mlflow.register_model(model_uri_v2, MODEL_NAME)

client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="champion",
    version=registered_v2.version,
)

print(f"Nueva version: {registered_v2.version}")
print(f"Alias 'champion' ahora apunta a version {registered_v2.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cargar modelo para inferencia
# MAGIC
# MAGIC Para cargar un modelo en produccion, usas el alias en vez del numero de version.
# MAGIC Asi, cuando promocionás una version nueva, todo el código que referencia `@champion`
# MAGIC automaticamente usa la version nueva sin cambiar nada.

# COMMAND ----------

# Cargar modelo por alias
model_champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")

# Predecir sobre datos nuevos
nuevos_datos = pd.DataFrame(
    {
        "sepal length (cm)": [5.1, 6.7, 4.9],
        "sepal width (cm)": [3.5, 3.1, 2.4],
        "petal length (cm)": [1.4, 5.6, 3.3],
        "petal width (cm)": [0.2, 2.4, 1.0],
    }
)

predicciones = model_champion.predict(nuevos_datos)

for i, pred in enumerate(predicciones):
    print(f"  Muestra {i+1}: clase {pred} ({iris.target_names[pred]})")

# COMMAND ----------

# MAGIC %md
# MAGIC Fijate que en ningún momento hardcodeamos un numero de version.
# MAGIC Si maniana registrás una version mejor y le asignas el alias `champion`,
# MAGIC este mismo código la usa sin tocar nada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cleanup
# MAGIC
# MAGIC Limpiamos todo lo que creamos para no dejar basura en el workspace.
# MAGIC
# MAGIC **Orden importante**: primero borrar las versiones del modelo, después el modelo registrado, y por ultimo el experimento.

# COMMAND ----------

# Listar todas las versiones del modelo
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
print(f"Versiones a borrar: {len(versions)}")

# Borrar alias primero (si no, no te deja borrar la version)
try:
    client.delete_registered_model_alias(MODEL_NAME, "champion")
    print("Alias 'champion' eliminado")
except Exception:
    pass

# Borrar cada version
for v in versions:
    client.delete_model_version(MODEL_NAME, v.version)
    print(f"  Version {v.version} eliminada")

# Borrar el modelo registrado
client.delete_registered_model(MODEL_NAME)
print(f"Modelo '{MODEL_NAME}' eliminado de Unity Catalog")

# COMMAND ----------

# Borrar el experimento
experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
if experiment:
    mlflow.delete_experiment(experiment.experiment_id)
    print(f"Experimento '{EXPERIMENT_NAME}' eliminado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC Lo que hicimos en este lab:
# MAGIC
# MAGIC | Paso | Que hicimos |
# MAGIC |------|-------------|
# MAGIC | Setup | Configurar MLflow para usar Unity Catalog como registry |
# MAGIC | Experimento | Crear un experimento para agrupar runs |
# MAGIC | Autolog | Entrenar un modelo y capturar todo automaticamente |
# MAGIC | Registro en UC | Registrar el modelo con nombre de tres niveles (`catalog.schema.modelo`) |
# MAGIC | Aliases | Asignar alias `champion` en vez de usar stages fijos |
# MAGIC | Lineage | Loguear datasets de entrada para crear lineage datos-modelo |
# MAGIC | Inferencia | Cargar modelo por alias para predecir |
# MAGIC | Cleanup | Eliminar versiones, modelo y experimento |
# MAGIC
# MAGIC Mas detalle en el [blog post](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-04-mlflow-unity-catalog/).
