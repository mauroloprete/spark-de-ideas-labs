# Docker en Databricks (DCS) — laboratorio practico

Lab del blog post [Databricks Tips #7: Docker en Databricks](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-06-docker-containers/).

Vas a construir una imagen Docker custom con librerias geoespaciales (GDAL) + Prophet, configurar un cluster de Databricks para usarla, y verificar que todo funciona.

## Requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- [Databricks CLI](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install) (v0.200+)
- Cuenta en [Databricks Free Edition](https://www.databricks.com/try-databricks) (gratis, sin tarjeta)
- DCS habilitado en tu workspace (ver paso 1)

## Estructura

```
docker-containers/
├── Dockerfile                       # Imagen custom con GDAL + Prophet
├── README.md                        # Este archivo
└── src/
    └── test_docker_container.py     # Notebook para verificar el contenedor
```

## 1. Habilitar DCS en tu workspace

Un workspace admin tiene que habilitar Container Services. Sin esto, el tab Docker no aparece.

**En Azure** (solo por CLI):

```bash
databricks workspace-conf set-status --json '{"enableDcs": "true"}' --profile <tu-profile>

# Verificar
databricks workspace-conf get-status enableDcs --profile <tu-profile>
# Tiene que devolver: { "enableDcs": "true" }
```

**En AWS**: Settings → Advanced → Container Services → Enabled.

## 2. Construir la imagen Docker

```bash
cd tips/docker-containers

# Build
docker build -t dcs-lab:v1 .

# Verificar que los imports funcionan
docker run --rm dcs-lab:v1 /databricks/python3/bin/python -c "
from osgeo import gdal
import geopandas
import prophet
print(f'GDAL {gdal.__version__}')
print('All imports OK')
"
```

## 3. Push al registry

Necesitas un Docker registry accesible desde tu workspace. Opciones:

### Docker Hub (mas simple para el lab)

```bash
# Login
docker login

# Tag + push
docker tag dcs-lab:v1 <tu-usuario>/dcs-lab:v1
docker push <tu-usuario>/dcs-lab:v1
```

### Azure Container Registry

```bash
# Login
az acr login --name <tu-acr>

# Tag + push
docker tag dcs-lab:v1 <tu-acr>.azurecr.io/dcs-lab:v1
docker push <tu-acr>.azurecr.io/dcs-lab:v1
```

## 4. Crear el cluster con Docker

### Desde la UI

1. Ir a Compute → Create compute
2. Elegir Databricks Runtime **16.4 LTS** (o superior)
3. Access mode: **Single User** o **No Isolation Shared**
4. Advanced Options → **Docker** tab
5. Seleccionar "Use your own Docker container"
6. Docker Image URL: `<tu-usuario>/dcs-lab:v1` (o la URL de ACR)
7. Crear el cluster

### Desde la API

```bash
databricks clusters create --json '{
  "cluster_name": "dcs-lab",
  "spark_version": "16.4.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 0,
  "docker_image": {
    "url": "<tu-usuario>/dcs-lab:v1"
  }
}' --profile <tu-profile>
```

> Si tu registry es privado, agrega `basic_auth` con usuario y password (idealmente usando Databricks Secrets).

## 5. Ejecutar el notebook

1. Importar `src/test_docker_container.py` en tu workspace
2. Adjuntarlo al cluster que creaste en el paso 4
3. Ejecutar todas las celdas

El notebook verifica:

- GDAL (libreria de sistema)
- GeoPandas + Shapely (operaciones geoespaciales)
- Prophet (forecasting con estacionalidad)
- Integracion con Spark y Delta Lake

## 6. Experimentar

Proba modificar el Dockerfile y reconstruir:

### Agregar una libreria

```dockerfile
# Agregar al final del Dockerfile
RUN /databricks/python3/bin/pip install --no-cache-dir plotly==6.0.1
```

```bash
docker build -t dcs-lab:v2 .
docker tag dcs-lab:v2 <tu-usuario>/dcs-lab:v2
docker push <tu-usuario>/dcs-lab:v2
```

Despues actualiza el cluster para usar `:v2` y reinicialo.

### Verificar el error del path

Proba instalar una libreria en el path equivocado para ver que pasa:

```dockerfile
# Esto NO va a funcionar en Databricks
RUN pip install requests==2.32.0
```

El `pip install` sin path completo instala en el Python del sistema, no en `/databricks/python3`. El notebook no va a encontrar la libreria.

## 7. Limpiar

```bash
# Eliminar el cluster desde la UI o:
databricks clusters delete --cluster-id <id> --profile <tu-profile>
```

## Links

- [Blog post — Databricks Tips #7](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-06-docker-containers/)
- [DCS para Dedicated Compute — Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/custom-containers)
- [DCS para Standard Compute — Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/custom-containers-standard)
- [Imagenes base — Docker Hub](https://hub.docker.com/u/databricksruntime)
