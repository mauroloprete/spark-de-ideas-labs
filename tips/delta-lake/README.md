# Delta Lake Tips — laboratorio practico

Lab del blog post [Databricks Tips #2: 7 cosas de Delta Lake que ojala me hubieran dicho antes](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-01-delta-lake/).

Aca vas a probar Liquid Clustering, OPTIMIZE con predicados, VACUUM, Time Travel y Change Data Feed en primera persona.
Todo funciona en **Databricks Free Edition** con Unity Catalog.

## Requisitos

- Cuenta en [Databricks Free Edition](https://www.databricks.com/try-databricks) (gratis, sin tarjeta)
- Un cluster activo con DBR 13.3+ (para Liquid Clustering)

## Contenido

```
delta-lake/
└── 01_delta_lake_tips.py   # Notebook con todos los ejercicios
```

## Como usarlo

1. Importa el notebook `01_delta_lake_tips.py` en tu workspace de Databricks
   - Workspace > Import > URL o arrastra el archivo
2. Conectalo a un cluster (Free Edition viene con uno)
3. Ejecuta las celdas en orden — cada seccion tiene explicaciones y codigo

## Que vas a practicar

1. **Setup** — Crear una tabla de transacciones con Delta Lake
2. **Liquid Clustering vs Z-ORDER** — Crear tabla con `CLUSTER BY`, cambiar columnas sin reescribir, comparar con Z-ORDER manual
3. **OPTIMIZE con predicados** — Diferencia entre optimizar toda la tabla vs solo una particion
4. **VACUUM** — Limpieza de archivos, diferencia entre data retention y log retention, `DESCRIBE HISTORY`
5. **Time Travel** — Consultar versiones anteriores, `RESTORE`, `SHALLOW CLONE`
6. **Change Data Feed (CDF)** — Activar CDF, hacer cambios, leer el feed con `table_changes()` y PySpark
7. **Cleanup** — Borrar las tablas del lab

## Notas

- El notebook usa el esquema `workspace.default` que viene disponible en Free Edition con Unity Catalog
- Al final del notebook hay un paso de cleanup que borra todas las tablas creadas
- Si alguna celda falla por permisos, verifica que tu cluster tenga Unity Catalog habilitado

## Links

- [Blog post](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-01-delta-lake/)
- [Documentacion de Delta Lake](https://docs.databricks.com/aws/en/delta/)
- [Liquid Clustering](https://docs.databricks.com/aws/en/delta/clustering)
- [Change Data Feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed)
