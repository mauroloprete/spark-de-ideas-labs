# Lab — Databricks Tips #16: Data Quality

Un pipeline de Lakeflow (Declarative Pipelines) con las reglas de calidad definidas **como datos** en una tabla Delta, el **Quarantine Pattern** oficial (columna `is_quarantined`, sin perder registros) y las métricas por regla consultadas del event log.

Post: [Databricks Tips #16: Data Quality](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-16-data-quality/)

## Qué hace

1. Crea una tabla bronze con 100.000 filas y datos sucios sembrados a propósito: 5.000 clientes nulos, 4.000 montos inválidos y 2.000 monedas desconocidas.
2. Define las reglas de calidad en `gobernanza.calidad.reglas` (una fila por regla, agrupadas por etiqueta).
3. Corre un pipeline serverless que carga las reglas con `get_rules("validez")`, las aplica con `@dp.expect_all` (warn: métricas sin filtrar) y separa válidos de cuarentena con la columna `is_quarantined`, leyendo la fuente una sola vez.
4. Consulta las métricas por regla en el event log y verifica el contrato del patrón: silver + cuarentena = bronze, exacto.

**Requisitos**: workspace con Unity Catalog, permisos `CREATE CATALOG` (o dos catálogos existentes donde crear los schemas) y serverless habilitado para el pipeline. Probado en un workspace Azure Premium con Serverless Starter Warehouse.

## Paso 1 — Seed

Corré [`src/seed.sql`](src/seed.sql) en un SQL Warehouse. Crea los catálogos (`lab`, `gobernanza`), la tabla de reglas y la bronze sucia. La verificación del final tiene que devolver:

```
total   monto_malo  cliente_nulo  moneda_mala
100000        4000          5000         2000
```

> Si tu metastore usa Default Storage (workspaces Express/serverless), el `CREATE CATALOG` pelado falla: agregá `MANAGED LOCATION` con la URL de una external location tuya, o creá los catálogos desde la UI.

## Paso 2 — El pipeline

1. Importá [`src/pipeline_dq.py`](src/pipeline_dq.py) como notebook (o archivo) en tu workspace.
2. Creá un pipeline con ese source, **serverless**, catálogo `lab`, schema `dq`, modo triggered.

   Con la CLI:

   ```bash
   databricks pipelines create --json '{
     "name": "lab-dq-tips16",
     "serverless": true,
     "catalog": "lab",
     "schema": "dq",
     "continuous": false,
     "development": true,
     "libraries": [{"notebook": {"path": "/Users/<tu-usuario>/lab-dq/pipeline_dq"}}]
   }'
   databricks pipelines start-update <pipeline_id>
   ```

3. Esperá el update (en el lab tardó menos de un minuto). El grafo: `lab.bronze.transacciones` → `transacciones_marcadas` → `silver_transacciones` + `cuarentena_transacciones`.

## Paso 3 — Métricas y verificación

Corré las dos queries de [`src/queries.sql`](src/queries.sql) (reemplazá `<pipeline_id>`). Salidas de referencia en [`outputs/`](outputs/):

**Métricas por regla** ([`outputs/metricas_event_log.txt`](outputs/metricas_event_log.txt)): cada regla con su contador, exactamente los rotos del seed.

**El contrato del patrón** ([`outputs/verificacion_conteo.txt`](outputs/verificacion_conteo.txt)): `92000 + 8000 = 100000`. Nada se pierde.

Ojo con la aritmética: las métricas suman 11.000 violaciones pero la cuarentena tiene 8.000 registros. Las métricas cuentan violaciones **por regla** y la cuarentena cuenta **registros**: un registro puede violar más de una regla (acá, los múltiplos de 50 violan dos y los de 100 violan las tres).

## Limpieza

```sql
DROP CATALOG lab CASCADE;
DROP CATALOG gobernanza CASCADE;
```

y borrá el pipeline `lab-dq-tips16` desde la UI o con `databricks pipelines delete <pipeline_id>`.
