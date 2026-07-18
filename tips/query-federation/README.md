# Lab — Databricks Tips #15: Query Federation

Monta un **foreign catalog** de Unity Catalog sobre un Postgres real (gratis, en Neon) y muestra con `EXPLAIN FORMATTED` qué parte de cada query se empuja a la base origen y cuál corre en el warehouse.

Post: [Databricks Tips #15: Query Federation](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-15-query-federation/)

## Qué hace

1. Crea un Postgres serverless gratis en [Neon](https://neon.tech) y lo llena con 5M de órdenes sintéticas (~300 MB, entra en el free tier).
2. Crea la `CONNECTION` y el `FOREIGN CATALOG` en Unity Catalog, con credenciales en un secret scope.
3. Corre tres queries con `EXPLAIN FORMATTED` para ver el pushdown en acción: una que se empuja entera (filtro + agregado), una que no se empuja (`levenshtein`), y el truco del `AND` (pushdown parcial).

**Requisitos**: workspace con Unity Catalog, permisos `CREATE CONNECTION` y `CREATE CATALOG` en el metastore, y un SQL Warehouse Pro o Serverless (canal 2023.40+). Probado en un workspace Premium con warehouse serverless.

## Paso 1 — La base en Neon

### Opción A: con la UI

1. Entrá a [console.neon.tech](https://console.neon.tech) y creá una cuenta (no pide tarjeta).
2. **New Project** → poné un nombre (ej. `spark-de-ideas-lab`), elegí la región y creá.
3. En el widget **Connect** del dashboard, elegí la **pooled connection** y copiá el connection string. Tiene esta forma:

   ```
   postgresql://<user>:<password>@<host>-pooler.<region>.aws.neon.tech/neondb?sslmode=require
   ```

   De ahí salen el `host`, el `user` y el `password` que vas a usar en los pasos siguientes.
4. Abrí el **SQL Editor** del proyecto, pegá el contenido de [`src/seed_neon.sql`](src/seed_neon.sql) y ejecutalo (la tabla se genera server-side, tarda ~10 segundos).

### Opción B: con la CLI

```bash
# Instalar y autenticar (abre el navegador)
brew install neonctl        # o: npm i -g neonctl
neonctl auth

# Crear el proyecto
neonctl projects create --name spark-de-ideas-lab

# Obtener el connection string (pooled)
neonctl connection-string --pooled

# Cargar los datos con psql
psql "$(neonctl connection-string --pooled)" -f src/seed_neon.sql
```

La verificación del final del seed tiene que devolver `5000000` filas y ~`300 MB`.

## Paso 2 — Secretos en Databricks

Las credenciales de Neon van a un secret scope, nunca en texto plano en el DDL:

```bash
databricks secrets create-scope lab-federation
databricks secrets put-secret lab-federation pg-user      # user de Neon
databricks secrets put-secret lab-federation pg-password  # password de Neon
```

## Paso 3 — Connection y foreign catalog

Corré [`src/federation.sql`](src/federation.sql) en un SQL Warehouse, reemplazando `<TU_HOST_NEON>` por el host de tu proyecto. El sanity check del final lee el `count(*)` en vivo contra Neon: `5000000`.

## Paso 4 — El pushdown en acción

Corré [`src/queries.sql`](src/queries.sql) y mirá los planes. La línea clave es `External engine query`: el SQL literal que viaja a Postgres.

### Resultados de referencia (serverless, canal 2025.x)

**Query A** (filtro + agregado): se empuja entera. Postgres agrega ~1,6M de filas y por el cable cruzan **3 filas**.

```
External engine query: SELECT "canal",COUNT(*) FROM "public"."orders"  WHERE ("fecha" IS NOT NULL) AND ("fecha" >= '2026-01-01') GROUP BY "canal"

tienda | 546281
app    | 537022
web    | 537022
```

**Query B** (`levenshtein`): no hay traducción posible. La subquery remota viaja casi desnuda y Postgres devuelve **los 5M de filas** por un único stream hacia un solo executor; el warehouse filtra después con un `PhotonFilter` local.

```
(3) PhotonFilter
    Arguments: (levenshtein(cliente#13548, cliente_42, None) <= 1)

External engine query: SELECT "order_id","cliente","canal","fecha","monto" FROM "public"."orders"  WHERE ("cliente" IS NOT NULL)
```

**Query C** (el truco del `AND`): pushdown parcial. La subquery remota lleva el filtro de fecha; el `levenshtein` queda en el `PhotonFilter` local.

**Bonus** (`ILIKE`): el ejemplo de filtro "no empujable" de la doc oficial. Al correrlo, el motor lo reescribe como `LOWER("cliente") LIKE '%tech%'` y lo empuja igual. Moraleja: el pushdown mejora con cada canal del warehouse; verificá contra tu plan, no contra la doc.

## Limpieza

```sql
DROP CATALOG pg_ventas CASCADE;
DROP CONNECTION pg_operacional;
```

```bash
databricks secrets delete-scope lab-federation
neonctl projects delete <project-id>   # o borrá el proyecto desde la UI
```
