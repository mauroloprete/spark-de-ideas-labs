# Structured Streaming en Databricks — laboratorio practico

Lab del blog post [Databricks Tips #4: Structured Streaming](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-03-structured-streaming/).

Aca vas a practicar los conceptos fundamentales de Structured Streaming:
triggers, Auto Loader, watermarks, foreachBatch con MERGE y monitoreo.
Todo funciona en **Databricks Free Edition**.

## Requisitos

- Cuenta en [Databricks Free Edition](https://www.databricks.com/try-databricks) (gratis, sin tarjeta)

## Notebook

| # | Notebook | Tema |
|---|----------|------|
| 1 | [01_structured_streaming.py](01_structured_streaming.py) | Triggers, Auto Loader, watermarks, foreachBatch + MERGE, monitoreo |

## Que cubre el lab

1. **Setup**: genera archivos JSON que simulan una landing zone de transacciones
2. **readStream basico**: lectura de JSON como stream con schema explicito
3. **Trigger modes**: `processingTime` vs `availableNow` y sus implicancias de costo
4. **Auto Loader**: `cloudFiles` con schema inference, schema evolution y rescue column
5. **Watermarks**: agregacion por ventanas de tiempo, tolerancia a late data
6. **foreachBatch + MERGE**: patron de upsert contra tablas Delta
7. **Monitoreo**: metricas clave (`numInputRows`, `processedRowsPerSecond`, `durationMs`)
8. **Cleanup**: detener streams, borrar tablas y checkpoints

## Como ejecutar

1. Importa el archivo `.py` en Databricks Free Edition
2. Crea un cluster (se asigna automaticamente en Free Edition)
3. Ejecuta las celdas en orden — el notebook genera sus propios datos

El notebook es auto-contenido: crea datos, los procesa y al final limpia todo.

## Links

- [Blog post](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-03-structured-streaming/)
- [Documentacion oficial de Structured Streaming](https://docs.databricks.com/aws/en/structured-streaming)
- [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-data-sources/auto-loader)
