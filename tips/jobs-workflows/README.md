# Jobs & Workflows — laboratorio practico

Lab del blog post [Databricks Tips #8: Jobs & Workflows — streaming y triggers que arrancan solos](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-08-jobs-workflows/).

Vas a configurar triggers event-driven (file arrival y table update), usar AutoLoader con `Trigger.AvailableNow`, y armar un pipeline bronze → silver con Change Data Feed.
Todo funciona en **Databricks Free Edition** con Unity Catalog.

## Requisitos

- Cuenta en [Databricks Free Edition](https://www.databricks.com/try-databricks) (gratis, sin tarjeta)
- Un cluster activo con DBR 15.4+ LTS
- Unity Catalog habilitado (viene por defecto en Free Edition)

## Estructura

```
jobs-workflows/
├── README.md                                # Este archivo
└── src/
    ├── 01_autoloader_file_arrival.py       # AutoLoader + AvailableNow + simulacion file arrival
    └── 02_microbatch_table_update.py       # Microbatch bronze→silver con CDF
```

## Como usarlo

### Lab 1: AutoLoader + File Arrival + AvailableNow

1. Importa `src/01_autoloader_file_arrival.py` en tu workspace
2. Conectalo a un cluster
3. Ejecuta las celdas en orden:
   - Simula archivos JSON llegando a un Volume
   - Ingesta con AutoLoader (`cloudFiles`) y `Trigger.AvailableNow`
   - Verifica exactly-once con checkpoints
   - Simula una segunda llegada de archivos y re-ejecuta

### Lab 2: Microbatch con Table Update + CDF

1. Ejecuta primero el Lab 1 (crea la tabla bronze)
2. Importa `src/02_microbatch_table_update.py` en tu workspace
3. Ejecuta las celdas en orden:
   - Habilita Change Data Feed en la tabla bronze
   - Lee los cambios con `readStream` + `readChangeFeed`
   - Transforma y escribe en silver con `AvailableNow`
   - Simula updates en bronze y re-procesa los cambios

## Que vas a practicar

1. **AutoLoader** — Ingesta incremental con `cloudFiles`, schema inference, schema evolution
2. **Trigger.AvailableNow** — Procesar todo lo pendiente y terminar (vs ProcessingTime que nunca para)
3. **Checkpoints** — Exactly-once processing, retomar desde donde quedo
4. **Change Data Feed** — Leer solo los cambios de una tabla Delta
5. **Pipeline bronze → silver** — Patron medallion con streaming incremental

## Notas

- Los notebooks usan el esquema `workspace.default` disponible en Free Edition
- Los archivos de ejemplo se crean en un Volume temporal dentro del notebook
- Al final de cada notebook hay un paso de cleanup que borra tablas y archivos
- Para probar los triggers (file arrival / table update) en un Job real, necesitas crear el Job manualmente desde la UI (ver instrucciones en los notebooks)

## Configurar triggers (opcional, no disponible en Free Edition)

Si tenes un workspace con Jobs habilitado:

### File Arrival Trigger

1. Crea un Job con el notebook `01_autoloader_file_arrival.py`
2. En Schedules & Triggers → Add trigger → File arrival
3. Storage location: `/Volumes/workspace/default/lab_bronze_vol/clientes/`
4. Usa **Job Cluster** (no All-Purpose) para ahorrar costos

### Table Update Trigger

1. Crea un Job con el notebook `02_microbatch_table_update.py`
2. En Schedules & Triggers → Add trigger → Table update
3. Tabla: `workspace.default.lab_clientes_bronze`
4. Trigger when: Any table is updated

## Links

- [Blog post — Databricks Tips #8](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-08-jobs-workflows/)
- [File arrival triggers — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/jobs/file-arrival-triggers)
- [Table update triggers — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/jobs/trigger-table-update)
- [AutoLoader — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
- [Change Data Feed — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed)
- [Structured Streaming triggers — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers)
