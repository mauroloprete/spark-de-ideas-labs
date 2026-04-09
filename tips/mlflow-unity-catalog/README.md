# MLflow + Unity Catalog -- laboratorio practico

Lab del blog post [Databricks Tips #5: MLflow + Unity Catalog](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-04-mlflow-unity-catalog/).

Aca vas a recorrer el flujo completo de MLflow integrado con Unity Catalog:
crear un experimento, entrenar un modelo con autolog, registrarlo en UC,
asignar aliases, ver lineage y cargar el modelo para inferencia.
Todo funciona en **Databricks Free Edition**.

## Requisitos

- Cuenta en [Databricks Free Edition](https://www.databricks.com/try-databricks) (gratis, sin tarjeta)
- Unity Catalog habilitado (viene por defecto en Free Edition)
- No hace falta instalar nada extra: `mlflow` y `sklearn` vienen preinstalados

## Contenido

```
mlflow-unity-catalog/
└── 01_mlflow_unity_catalog.py   # Notebook con el lab completo
```

## Como usarlo

1. Importa el notebook (`.py`) en tu workspace de Databricks
2. Asegurate de estar conectado a un cluster con Unity Catalog
3. Ejecuta las celdas en orden de arriba hacia abajo

## Que vas a ver

| Seccion | Que hace |
|---------|----------|
| Setup | Configura MLflow para usar Unity Catalog como registry |
| Experimento | Crea un experimento para agrupar runs |
| Autolog | Entrena un RandomForest y captura params/metricas/modelo automaticamente |
| Registro en UC | Registra el modelo con nombre de tres niveles (`main.default.lab_iris_model`) |
| Aliases | Asigna alias `champion` en vez de los stages fijos del registry viejo |
| Lineage | Loguea datasets de entrada para crear lineage datos-a-modelo |
| Inferencia | Carga el modelo por alias y predice sobre datos nuevos |
| Cleanup | Elimina todo lo creado para no dejar basura |

## Links

- [Blog post](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-04-mlflow-unity-catalog/)
- [Documentacion oficial de MLflow en Databricks](https://docs.databricks.com/aws/en/mlflow)
- [Unity Catalog para modelos](https://docs.databricks.com/aws/en/mlflow/models-in-uc)
