# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Docker en Databricks (DCS)
# MAGIC
# MAGIC Este notebook verifica que tu contenedor Docker custom funciona correctamente en Databricks.
# MAGIC
# MAGIC > Requiere un cluster con Databricks Container Services habilitado y la imagen Docker del lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar librerías de sistema (GDAL)
# MAGIC
# MAGIC Estas librerías vienen pre-instaladas en la imagen Docker.
# MAGIC Si este paso falla, el Dockerfile no se construyó correctamente.

# COMMAND ----------

from osgeo import gdal

print(f"GDAL version: {gdal.__version__}")
print("GDAL se linkeó correctamente desde la imagen Docker")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar stack geoespacial

# COMMAND ----------

import geopandas as gpd
import shapely
from shapely.geometry import Point, Polygon

print(f"GeoPandas: {gpd.__version__}")
print(f"Shapely:   {shapely.__version__}")

# Crear geometrías de ejemplo
zonas = gpd.GeoDataFrame({
    "zona_id":  ["centro", "norte", "sur", "este", "oeste"],
    "nombre":   ["Centro", "Zona Norte", "Zona Sur", "Zona Este", "Zona Oeste"],
    "geometry": [
        Point(-56.1882, -34.9011),  # Montevideo centro
        Point(-56.1700, -34.8500),
        Point(-56.2000, -34.9500),
        Point(-56.1200, -34.9000),
        Point(-56.2500, -34.9100),
    ]
})

# Buffer de 0.01 grados (~1km) alrededor de cada punto
zonas["geometry"] = zonas.geometry.buffer(0.01)
print(f"\nZonas creadas: {len(zonas)}")
display(zonas.drop(columns="geometry"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Operaciones geoespaciales
# MAGIC
# MAGIC Verificamos que las operaciones de intersección y área funcionan.

# COMMAND ----------

# Punto de prueba: ¿en qué zona cae?
punto_test = Point(-56.1882, -34.9011)

for _, zona in zonas.iterrows():
    if zona.geometry.contains(punto_test):
        print(f"El punto cae en: {zona['nombre']}")

# Calcular áreas
zonas["area_km2"] = zonas.geometry.area * 111**2  # Aproximación en km2
display(zonas[["zona_id", "nombre", "area_km2"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar Prophet (forecasting)

# COMMAND ----------

from prophet import Prophet
import pandas as pd
import numpy as np

# Generar serie temporal sintética (demanda diaria por zona)
np.random.seed(42)
fechas = pd.date_range("2025-01-01", periods=365, freq="D")

demanda = pd.DataFrame({
    "ds": fechas,
    "y": (
        100
        + 20 * np.sin(2 * np.pi * np.arange(365) / 7)     # estacionalidad semanal
        + 50 * np.sin(2 * np.pi * np.arange(365) / 365)    # estacionalidad anual
        + np.random.normal(0, 10, 365)                      # ruido
    )
})

print(f"Serie temporal: {len(demanda)} observaciones")
print(f"Rango: {demanda['ds'].min().date()} a {demanda['ds'].max().date()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Forecast con Prophet

# COMMAND ----------

modelo = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False
)
modelo.fit(demanda)

futuro = modelo.make_future_dataframe(periods=30)
forecast = modelo.predict(futuro)

# Mostrar predicciones de los próximos 30 días
predicciones = forecast[forecast["ds"] > demanda["ds"].max()][["ds", "yhat", "yhat_lower", "yhat_upper"]]
print(f"\nPredicciones generadas: {len(predicciones)} días")

# Convertir a Spark DataFrame y mostrar
spark_forecast = spark.createDataFrame(predicciones)
display(spark_forecast)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Integración con Delta Lake
# MAGIC
# MAGIC Guardamos las predicciones como Delta table (usa el catálogo por defecto).

# COMMAND ----------

from pyspark.sql import functions as F

# Agregar metadata
spark_forecast_enriched = (
    spark_forecast
    .withColumn("zona_id", F.lit("centro"))
    .withColumn("modelo", F.lit("prophet_v1"))
    .withColumn("generado_en", F.current_timestamp())
)

# Guardar como tabla temporal (no requiere Unity Catalog)
spark_forecast_enriched.write.mode("overwrite").saveAsTable("dcs_lab_forecast")

print("Tabla 'dcs_lab_forecast' creada exitosamente")
display(spark.table("dcs_lab_forecast"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Limpiar

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS dcs_lab_forecast")
print("Tabla eliminada. Lab completado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado
# MAGIC
# MAGIC Si llegaste hasta acá sin errores, tu contenedor Docker funciona perfectamente en Databricks.
# MAGIC
# MAGIC Verificaste:
# MAGIC - GDAL (librería de sistema, instalada via `apt-get` en el Dockerfile)
# MAGIC - GeoPandas + Shapely (operaciones geoespaciales)
# MAGIC - Prophet (forecasting con estacionalidad)
# MAGIC - Integración con Spark y Delta Lake
# MAGIC
# MAGIC Ahora probá:
# MAGIC - Agregar más librerías al `Dockerfile` y reconstruir la imagen
# MAGIC - Configurar CI/CD para buildear automáticamente
# MAGIC - Usar Databricks Secrets para las credenciales del registry
