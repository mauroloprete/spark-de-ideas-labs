# Unity Catalog -- laboratorio practico

Lab del blog post [Databricks Tips #3: Unity Catalog](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-02-unity-catalog/).

Aca vas a crear un catalog completo con gobernanza: GRANTS, row-level security, column masking, volumes y linaje automatico.
Todo funciona en **Databricks Free Edition**.

## Requisitos

- Cuenta en [Databricks Free Edition](https://www.databricks.com/try-databricks) (gratis, sin tarjeta)
- Unity Catalog habilitado (viene por defecto en Free Edition)

## Notebook

| # | Notebook | Tema |
|---|----------|------|
| 1 | [01_unity_catalog_governance.py](01_unity_catalog_governance.py) | Namespace de 3 niveles, GRANTS, row/column security, volumes, linaje |

## Como ejecutar

1. Importa el archivo `.py` en Databricks Free Edition (Workspace > Import)
2. Conecta un cluster (se asigna automaticamente en Free Edition)
3. Ejecuta las celdas en orden
4. Al final, ejecuta la celda de cleanup para borrar todo lo creado

## Que vas a hacer

1. Crear un catalog `lab_unity_catalog` con schemas bronze/silver/gold
2. Aplicar GRANTS granulares (el patron correcto vs. el error comun)
3. Configurar row-level security con funciones de filtrado por region
4. Aplicar column masking para proteger emails y SSN
5. Crear un managed volume y subir/leer archivos
6. Generar linaje automatico creando tablas silver y gold derivadas
7. Limpiar todos los objetos creados

## Links

- [Blog post](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-02-unity-catalog/)
- [Documentacion oficial de Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
