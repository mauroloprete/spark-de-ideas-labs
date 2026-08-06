from pyspark import pipelines as dp
from pyspark.sql import functions as F


def get_rules(etiqueta):
    filas = (
        spark.read.table("gobernanza.calidad.reglas")
        .filter(F.col("etiqueta") == etiqueta)
        .collect()
    )
    return {fila["nombre"]: fila["condicion"] for fila in filas}


reglas = get_rules("validez")

# Un registro va a cuarentena si NO cumple todas las reglas
condicion_cuarentena = "NOT({0})".format(
    " AND ".join(f"({c})" for c in reglas.values())
)


@dp.table(partition_cols=["is_quarantined"])
@dp.expect_all(reglas)  # warn: deja métricas, no filtra
def transacciones_marcadas():
    return (
        spark.readStream.table("lab.bronze.transacciones")
        .withColumn("is_quarantined", F.expr(condicion_cuarentena))
    )


@dp.table
def silver_transacciones():
    return (
        spark.readStream.table("transacciones_marcadas")
        .filter("is_quarantined = false")
        .drop("is_quarantined")
    )


@dp.table
def cuarentena_transacciones():
    return (
        spark.readStream.table("transacciones_marcadas")
        .filter("is_quarantined = true")
    )
