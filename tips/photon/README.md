# Lab — Databricks Tips #12: Photon

Benchmark **con/sin Photon**: mismo notebook, mismo node type, dos job clusters
que solo difieren en `runtime_engine` (`PHOTON` vs `STANDARD`).

> ⚠️ Este lab necesita **classic compute** para poder apagar Photon — no corre en
> Free Edition (serverless-only, Photon siempre activo).

## Qué hace

1. Genera 200M de filas sintéticas estilo TPC-DS en `<catalog>.<schema>.store_sales` (idempotente)
2. Corre una query CPU-bound (aggregation + `COUNT(DISTINCT)` + window function) 5 veces con sink `noop`
3. Reporta la mediana de wall-clock y el plan de ejecución (para ver los nodos `Photon*`)

## Cómo correrlo

```bash
cd tips/photon
databricks bundle deploy
databricks bundle run benchmark_standard   # primero: crea la tabla
databricks bundle run benchmark_photon     # después: reusa la tabla
```

Variables (`--var`): `catalog` (default `dev_bronze`), `schema` (`photon_bench`),
`node_type` (`Standard_D4ds_v5`), `spark_version` (`17.3.x-scala2.13`).

## Qué mirar

- La **mediana de wall-clock** de cada job (sale por `dbutils.notebook.exit` como JSON)
- El **plan de ejecución**: `PhotonScan`/`PhotonGroupingAgg` vs `FileScan`/`HashAggregate`
- El **Spark UI → SQL/DataFrame**: operadores Photon en naranja, Spark en azul

Blog post: [Databricks Tips #12: Photon](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-12-photon/)
