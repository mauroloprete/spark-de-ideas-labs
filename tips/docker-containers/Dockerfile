# Dockerfile para el lab de DCS

FROM databricksruntime/standard:16.4-LTS


RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*


RUN /databricks/python3/bin/pip install --no-cache-dir \
    geopandas==1.0.1 \
    shapely==2.0.6 \
    fiona==1.10.1 \
    prophet==1.1.6 \
    great-expectations==1.3.0


RUN /databricks/python3/bin/python -c "from osgeo import gdal; print(f'GDAL {gdal.__version__}')"


RUN /databricks/python3/bin/python -c "\
import geopandas; \
import shapely; \
import prophet; \
import great_expectations; \
print('All imports OK')"
