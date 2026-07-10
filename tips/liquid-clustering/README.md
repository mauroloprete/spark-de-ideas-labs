# Lab — Databricks Tips #14: Liquid Clustering

Compara tres layouts sobre los **mismos** datos y mide cuánto poda cada uno con la misma query:

- **Particionado por fecha** (`PARTITIONED BY`)
- **Z-ORDER** (`OPTIMIZE ... ZORDER BY`)
- **Liquid Clustering** (`CLUSTER BY`)

Corre en **Databricks Free Edition** (serverless, Photon siempre activo).

## Qué hace

1. Genera 200M de filas sintéticas en `<catalog>.<schema>.ventas_base` (idempotente). `cliente` tiene alta cardinalidad (50.000 valores).
2. Crea las tres tablas con los tres layouts y corre `OPTIMIZE` (para Liquid, `OPTIMIZE FULL`).
3. Reporta, por estrategia, el **total de archivos** (`DESCRIBE DETAIL`) y el **wall-clock** de la query filtrada.
4. Documenta cómo sacar **files read / files pruned** de forma reproducible.

## Cómo correrlo

```bash
cd tips/liquid-clustering
databricks bundle deploy
databricks bundle run liquid_clustering_benchmark
```

Variables (`--var`): `catalog` (default `main`), `schema` (`liquid_clustering_lab`), `rows` (`200000000`).

## Cómo se sacan los resultados (reproducible)

El **total de archivos** y el **wall-clock** salen del notebook (por `dbutils.notebook.exit`).

Los **archivos leídos y podados** no se pueden leer del plan en serverless (Spark Connect). Hay dos caminos oficiales, los dos reproducibles:

**A) Query profile (UI).** Corré una query y abrí el query profile: los campos **Files pruned** y **Files read** están ahí. Ideal para un screenshot.

**B) Query history (programático).** Corré `src/queries.sql` en un SQL Warehouse y leé las métricas del history:

```bash
databricks api get /api/2.0/sql/history/queries \
  --json '{"include_metrics": true, "max_results": 20}' \
  | jq -r '.res[]
      | select(.query_text | test("ventas_(part|zorder|liquid)"))
      | "\(.query_text | capture("ventas_(?<t>[a-z]+)").t)  read_files=\(.metrics.read_files_count)  pruned_files=\(.metrics.pruned_files_count)  read_bytes=\(.metrics.read_bytes)"'
```

## Resultados de referencia (200M filas, serverless + Photon)

| Estrategia | Archivos totales | Archivos leídos | Archivos podados | Bytes leídos |
|---|---|---|---|---|
| Particionado por fecha | 600 | 30 | 570 | 119 MB |
| Z-ORDER (cliente, fecha) | 36 | 1 | 35 | 20 MB |
| Liquid Clustering (cliente, fecha) | 36 | 1 | 35 | 15 MB |

La query filtra por `cliente` **y** `fecha`. El particionado poda por fecha (de 600 baja a 30 archivos) pero no puede podar por `cliente`, así que los lee enteros. Z-ORDER y Liquid podan por las dos y bajan a 1 archivo.

Post completo: https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-14-liquid-clustering/
