# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Lab Lakehouse//RT (Tips #18)
# MAGIC
# MAGIC Crea el dataset de serving: `clientes` y `pedidos` como **tablas managed de
# MAGIC Unity Catalog** (la Beta de Lakehouse//RT no soporta external tables, Hive
# MAGIC metastore ni formatos que no sean Delta/Iceberg). `pedidos` queda con
# MAGIC Liquid Clustering por `(cliente_id, ts)`, que es el patrón de acceso del
# MAGIC benchmark. Idempotente: si las tablas ya existen con el tamaño pedido, no
# MAGIC recrea nada.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_bronze")
dbutils.widgets.text("schema", "lakehouse_rt_tips18")
dbutils.widgets.text("n_clientes", "200000")
dbutils.widgets.text("n_pedidos", "20000000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
n_clientes = int(dbutils.widgets.get("n_clientes"))
n_pedidos = int(dbutils.widgets.get("n_pedidos"))

fq = f"{catalog}.{schema}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq}")

# Estas listas tienen que coincidir con los pools de parámetros de
# bench/bench.py: si cambia una, cambia la otra.
CATEGORIAS = "'hogar','electro','deporte','libreria','juguetes','jardin'"
PAISES = "'UY','AR','CL','PY','BR','MX','PE','CO'"


def tabla_lista(nombre: str, n_min: int) -> bool:
    if not spark.catalog.tableExists(f"{fq}.{nombre}"):
        return False
    return spark.table(f"{fq}.{nombre}").count() >= n_min

# COMMAND ----------

if tabla_lista("clientes", n_clientes):
    print(f"{fq}.clientes ya existe con >= {n_clientes:,} filas, no se recrea")
else:
    spark.sql(f"DROP TABLE IF EXISTS {fq}.clientes")
    spark.sql(f"""
        CREATE TABLE {fq}.clientes AS
        SELECT
            id                                                        AS cliente_id,
            element_at(array('retail','corporativo','premium'),
                       CAST(1 + pmod(id, 3) AS INT))                  AS segmento,
            element_at(array({PAISES}),
                       CAST(1 + pmod(id, 8) AS INT))                  AS pais,
            date_sub(current_date(), CAST(rand() * 1460 AS INT))      AS fecha_alta
        FROM range(1, {n_clientes} + 1)
    """)
    print(f"{fq}.clientes creada: {n_clientes:,} filas")

# COMMAND ----------

if tabla_lista("pedidos", n_pedidos):
    print(f"{fq}.pedidos ya existe con >= {n_pedidos:,} filas, no se recrea")
else:
    spark.sql(f"DROP TABLE IF EXISTS {fq}.pedidos")
    spark.sql(f"""
        CREATE TABLE {fq}.pedidos
        CLUSTER BY (cliente_id, ts) AS
        SELECT
            id                                                        AS pedido_id,
            1 + CAST(rand() * ({n_clientes} - 1) AS INT)              AS cliente_id,
            concat('P-', lpad(CAST(1 + CAST(rand() * 199 AS INT)
                              AS STRING), 3, '0'))                    AS producto,
            element_at(array({CATEGORIAS}),
                       1 + CAST(rand() * 5.999 AS INT))               AS categoria,
            round(rand() * 500 + 5, 2)                                AS monto,
            element_at(array('pagado','enviado','entregado','cancelado'),
                       1 + CAST(rand() * 3.999 AS INT))               AS estado,
            timestampadd(SECOND, -CAST(rand() * 30 * 86400 AS INT),
                         current_timestamp())                         AS ts
        FROM range(1, {n_pedidos} + 1)
    """)
    spark.sql(f"OPTIMIZE {fq}.pedidos")
    print(f"{fq}.pedidos creada y clusterizada: {n_pedidos:,} filas")

# COMMAND ----------

# Tabla descartable para el bloque de limitaciones (workspace/03_limitaciones.sql):
# el INSERT de ese script apunta acá para que un despiste conectado al warehouse
# equivocado no ensucie el dataset del benchmark.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {fq}.demo_escritura (
        id INT, nota STRING
    )
""")
spark.sql(f"""
    INSERT OVERWRITE {fq}.demo_escritura
    VALUES (1, 'tabla de sacrificio para demos de escritura')
""")

# COMMAND ----------

import json

resumen = {
    "clientes": spark.table(f"{fq}.clientes").count(),
    "pedidos": spark.table(f"{fq}.pedidos").count(),
    "schema": fq,
}
print(json.dumps(resumen))
dbutils.notebook.exit(json.dumps(resumen))
