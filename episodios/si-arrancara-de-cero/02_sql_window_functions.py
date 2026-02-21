# Databricks notebook source
# MAGIC %md
# MAGIC # SQL: el lenguaje que nunca muere
# MAGIC
# MAGIC Notebook del episodio **"Si arrancara de cero"** del podcast [Spark de Ideas](https://open.spotify.com/show/20WqZma0iLDYbg9wAEfv0w).
# MAGIC
# MAGIC Acá practicamos window functions, CTEs y JOINs avanzados con datos de ejemplo.
# MAGIC
# MAGIC > Ejecutable en Databricks Free Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: crear datos de ejemplo

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import date, timedelta
import random

random.seed(42)

# Generar datos de ventas
rows = []
customers = list(range(1, 10))
for day_offset in range(30):
    d = date(2026, 1, 1) + timedelta(days=day_offset)
    for cust in random.sample(customers, k=random.randint(2, 5)):
        rows.append((
            cust,
            float(random.randint(10, 500)),
            d,
            random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "FAILED"])
        ))

sales_df = spark.createDataFrame(rows, ["customer_id", "amount", "sale_date", "status"])
sales_df.createOrReplaceTempView("sales")

print(f"Filas generadas: {sales_df.count()}")
sales_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Window Functions — lo que separa junior de senior
# MAGIC
# MAGIC `ROW_NUMBER()`, `LAG()`, `LEAD()`, `RANK()`, `SUM() OVER()`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ventas diarias por cliente con ranking y cambio vs día anterior
# MAGIC WITH daily_sales AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         sale_date AS sale_day,
# MAGIC         SUM(amount) AS daily_total,
# MAGIC         COUNT(*) AS txn_count
# MAGIC     FROM sales
# MAGIC     WHERE status = 'COMPLETED'
# MAGIC     GROUP BY 1, 2
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY customer_id
# MAGIC             ORDER BY daily_total DESC
# MAGIC         ) AS rn,
# MAGIC         LAG(daily_total) OVER (
# MAGIC             PARTITION BY customer_id
# MAGIC             ORDER BY sale_day
# MAGIC         ) AS prev_day_total
# MAGIC     FROM daily_sales
# MAGIC )
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     sale_day,
# MAGIC     daily_total,
# MAGIC     prev_day_total,
# MAGIC     ROUND(
# MAGIC         (daily_total - prev_day_total) / prev_day_total * 100, 2
# MAGIC     ) AS pct_change
# MAGIC FROM ranked
# MAGIC WHERE rn <= 5
# MAGIC ORDER BY customer_id, rn

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Running totals con SUM() OVER()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total acumulado por cliente
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     sale_date,
# MAGIC     amount,
# MAGIC     SUM(amount) OVER (
# MAGIC         PARTITION BY customer_id
# MAGIC         ORDER BY sale_date
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS running_total
# MAGIC FROM sales
# MAGIC WHERE status = 'COMPLETED'
# MAGIC ORDER BY customer_id, sale_date
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. RANK vs DENSE_RANK vs ROW_NUMBER

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Diferencia entre los tres
# MAGIC WITH totals AS (
# MAGIC     SELECT customer_id, SUM(amount) AS total
# MAGIC     FROM sales
# MAGIC     WHERE status = 'COMPLETED'
# MAGIC     GROUP BY customer_id
# MAGIC )
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     total,
# MAGIC     ROW_NUMBER() OVER (ORDER BY total DESC) AS row_num,
# MAGIC     RANK() OVER (ORDER BY total DESC) AS rank,
# MAGIC     DENSE_RANK() OVER (ORDER BY total DESC) AS dense_rank
# MAGIC FROM totals

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. CTEs — queries legibles
# MAGIC
# MAGIC Regla: si tu query tiene más de 2 niveles de anidamiento, refactorizalo con CTEs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clientes con más de 3 transacciones completadas
# MAGIC -- cuyo promedio diario supera los $100
# MAGIC WITH completed AS (
# MAGIC     SELECT * FROM sales WHERE status = 'COMPLETED'
# MAGIC ),
# MAGIC daily_avg AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(DISTINCT sale_date) AS active_days,
# MAGIC         COUNT(*) AS total_txns,
# MAGIC         ROUND(AVG(amount), 2) AS avg_amount
# MAGIC     FROM completed
# MAGIC     GROUP BY customer_id
# MAGIC     HAVING COUNT(*) > 3
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM daily_avg
# MAGIC WHERE avg_amount > 100
# MAGIC ORDER BY avg_amount DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. JOINs avanzados — ANTI y SEMI

# COMMAND ----------

# Crear tabla de clientes VIP
vip_df = spark.createDataFrame([(1,), (3,), (99,)], ["customer_id"])
vip_df.createOrReplaceTempView("vip_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SEMI JOIN: clientes que SÍ están en la lista VIP y tienen ventas
# MAGIC SELECT DISTINCT s.customer_id, COUNT(*) as txns
# MAGIC FROM sales s
# MAGIC LEFT SEMI JOIN vip_customers v ON s.customer_id = v.customer_id
# MAGIC GROUP BY s.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANTI JOIN: clientes que NO están en la lista VIP
# MAGIC SELECT DISTINCT s.customer_id, COUNT(*) as txns
# MAGIC FROM sales s
# MAGIC LEFT ANTI JOIN vip_customers v ON s.customer_id = v.customer_id
# MAGIC GROUP BY s.customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC - **Window functions**: resolvés en una query lo que antes eran 3 scripts de Python
# MAGIC - **CTEs**: queries legibles, no anidados de 200 líneas
# MAGIC - **SEMI/ANTI JOINs**: filtrar por existencia sin duplicar filas
# MAGIC - SQL tiene +50 años y sigue siendo el lenguaje más usado en datos
