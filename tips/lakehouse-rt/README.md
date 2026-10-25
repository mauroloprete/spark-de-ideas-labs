# Lab — Lakehouse//RT vs SQL Serverless

Benchmark de latencia de **Lakehouse Real-Time** (Beta), el warehouse tipo
`REALTIME`, contra un SQL warehouse **serverless** común, medido **desde fuera**
vía Statement Execution API. Ambos warehouses Small, mismo tamaño a propósito.

Es el lab del post [Databricks Tips #18: Lakehouse//RT](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-18-lakehouse-rt/)
de Spark de Ideas. Los números que publiqué ahí salen de este código; es una
Beta, así que lo que midas vos puede diferir.

## Requisitos

- Un workspace con la Beta de Lakehouse RT habilitada (Previews del workspace →
  "Lakehouse RT") y un warehouse Real-Time creado.
- Un SQL warehouse serverless para comparar. Ojo: la API lo reporta
  `warehouse_type: PRO`; lo que lo distingue es `enable_serverless_compute`,
  y el harness lo etiqueta `SERVERLESS`.
- Python 3.11+ con `databricks-sdk`, R con `ggplot2` (`shiny` y `plotly` para
  el panel en vivo), y el CLI de Databricks autenticado.

## Qué crea

- `<catalog>.<schema>.pedidos`: 20M de pedidos sintéticos, tabla managed de
  Unity Catalog con Liquid Clustering por `(cliente_id, ts)`.
- `<catalog>.<schema>.clientes`: 200k clientes (segmento, país).
- `demo_escritura`: tabla de sacrificio para el script de limitaciones.
- Job `lakehouse_rt_lab_setup` (serverless): todo lo anterior, idempotente.
  El setup corre en un serverless común porque el warehouse RT es solo-SELECT.

## Pasos

```bash
cd tips/lakehouse-rt
databricks bundle deploy -t dev -p <perfil>
databricks bundle run setup -t dev -p <perfil>

# Benchmark (arranca los dos warehouses si están apagados):
python3 bench/bench.py \
  --rt <warehouse_id_realtime> \
  --serverless <warehouse_id_serverless> \
  --niveles 1,4,16 --corridas 24 --warmup 5
```

Salida: `bench/results/latencias.csv` (el crudo) más un resumen p50/p95/p99
por warehouse × query × concurrencia en consola. Los PNG salen de
`Rscript bench/graficos.R`.

## Las tres series

La misma corrida mide tres series intercaladas:

1. **Lakehouse//RT** con parámetros aleatorios.
2. **SQL Serverless** con parámetros aleatorios: cada request lleva además un
   nonce proyectado como columna, porque el result cache matchea por
   fingerprint del plan (un nonce en comentario se descarta al parsear) y con
   un resultado distinto por corrida ningún cache puede servir uno anterior.
3. **SQL Serverless (cache)**: el mismo warehouse con las queries idénticas y
   literales fijos, para medir qué aporta el result cache cuando la query se
   repite igual.

La columna `result_from_cache` del Query History queda en el CSV como
auditoría: 0 en las dos primeras series, ~1 en la tercera. Si tu corrida
muestra otra cosa, los números no valen.

## Panel en vivo

El CSV se reescribe corrida a corrida (escritura atómica) y las métricas
server-side se completan a medida que el Query History las publica
(~15-30 s de retraso). En una terminal el panel, en otra el bench:

```bash
Rscript bench/live.R          # abre http://localhost:4321
python3 bench/bench.py --rt <id_rt> --serverless <id_serverless>
```

## Concurrencia alta (hasta 1000 en vuelo)

```bash
python3 bench/bench.py --rt <id_rt> --serverless <id_serverless> \
  --niveles 1,16,64,256,1000
```

- En cada nivel se lanzan `max(--corridas, nivel)` requests: a 1000 son 1000
  requests en vuelo de verdad.
- La ventana sincrónica de la API corta a los 50 s; después el harness sigue
  por polling hasta estado terminal, así las colas largas no se pierden.
- A estos niveles el `client_ms` mide harness + red, no el motor: el número
  comparable sigue siendo el server-side del Query History.
- Esto **no** reproduce el benchmark de 12.000 QPS de Databricks: son hasta
  1000 requests desde una laptop contra warehouses Small.
- Ojo al costo: el serverless encola y puede escalar para drenar; una celda
  de 1000 puede tardar varios minutos.

## Metodología: los dos números

- **client_ms**: wall-clock del request completo a `/api/2.0/sql/statements`.
  Incluye red y overhead de API (un `SELECT 1` de baseline, que el harness
  re-mide en cada corrida, ya cuesta cientos de ms según tu distancia a la
  región). Sirve para comparar motores entre sí, no para validar el sub-100.
- **total/execution/compilation/fetch (ms)**: métricas server-side del Query
  History por `statement_id`. Ese es el número comparable contra el claim
  público. El warm-up se descarta (5 corridas por query).

Las latencias de este lab son mediciones propias y se publican como tales,
nunca mezcladas con los números oficiales de Databricks.

## Queries del benchmark

| Query | Patrón | Parámetro |
|-------|--------|-----------|
| `q1_estado_cliente` | lookup puntual por cliente (pruning por Liquid Clustering) | `cliente_id` |
| `q2_top_productos` | panel 24 h: top 10 por categoría | `categoria` |
| `q3_panel_segmento` | join + agregación 7 días por país | `pais` |

En `workspace/` están las versiones para el SQL editor: **01** el dataset,
**02** las mismas 3 queries con literales fijos y las variantes para ver el
efecto del result cache, **03** las limitaciones de la Beta (correr SOLO
conectado al warehouse RT: el error es el output; en un serverless común esos
statements sí corren), **04** el costo por SKU desde las system tables
(correr en el serverless: no están soportadas en RT, y los registros de
billing demoran horas).

## Costo estimado

- Setup (20M filas + OPTIMIZE en serverless): pocos minutos de job, ~1-2 DBU.
- Benchmark estándar: dos warehouses Small ~20-30 min ≈ 8-12 DBU en total
  (el RT emite 30% menos DBUs hasta el 31/1/2027; verificar con `04_costos`).
- Con `--niveles ...,1000` cada nivel suma miles de queries por serie y el
  serverless puede escalar para drenar la cola: esperá bastantes más DBUs.
- Auto-stop recomendado: RT a 5 min, serverless a 10; no quedan prendidos.
