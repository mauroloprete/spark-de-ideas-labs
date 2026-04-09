# Patrones de Ingesta en Data Engineering -- laboratorio practico

Lab del blog post [Data Engineering Design Patterns: 8 patrones de ingesta](https://mauroloprete.github.io/mauroloprete/blog/posts/data-engineering-design-patterns/).

Implementacion practica de 4 patrones de ingesta con datos de ejemplo: Full Loader, Incremental Loader, CDC con MERGE INTO y Compactor (OPTIMIZE). Todo funciona en **Databricks Free Edition**.

## Notebooks

| # | Notebook | Tema |
|---|----------|------|
| 1 | [01_ingestion_patterns.py](01_ingestion_patterns.py) | Full Loader, Incremental Loader, CDC con MERGE, Compactor |

## Que vas a ver

- **Full Loader**: carga completa con `mode("overwrite")`. Lo mas simple pero lo mas costoso.
- **Incremental Loader**: carga solo lo nuevo usando high watermark y tabla de control.
- **CDC con MERGE INTO**: aplicar inserts, updates y deletes con deduplicacion via `ROW_NUMBER()`.
- **Compactor**: simular micro-batches, contar archivos, ejecutar `OPTIMIZE` y ver la reduccion.

## Como ejecutar

1. Importa el archivo `.py` en Databricks Free Edition
2. Crea un cluster (se asigna automaticamente en Free Edition)
3. Ejecuta las celdas en orden -- el notebook crea y limpia sus propios datos

## Links

- [Blog post](https://mauroloprete.github.io/mauroloprete/blog/posts/data-engineering-design-patterns/)
- [Documentacion de MERGE en Delta Lake](https://docs.delta.io/latest/delta-update.html)
- [Documentacion de OPTIMIZE](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html)
