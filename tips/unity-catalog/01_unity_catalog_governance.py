# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog: gobernanza de datos en la practica
# MAGIC
# MAGIC Notebook del blog post [Databricks Tips #3: Unity Catalog](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-tips-02-unity-catalog/).
# MAGIC
# MAGIC Aca vas a crear catalogs, schemas, aplicar GRANTS, configurar row-level security,
# MAGIC column masking, volumes y ver linaje automatico. Todo funciona en **Databricks Free Edition**.
# MAGIC
# MAGIC > Ejecuta las celdas en orden. Al final hay un bloque de limpieza que borra todo lo creado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. El modelo de 3 niveles
# MAGIC
# MAGIC Unity Catalog organiza los datos en un namespace de 3 niveles:
# MAGIC
# MAGIC ```
# MAGIC metastore
# MAGIC   +-- catalog          (ambiente o dominio)
# MAGIC        +-- schema      (area funcional)
# MAGIC             +-- table  (dato)
# MAGIC ```
# MAGIC
# MAGIC Cada objeto vive adentro de esta jerarquia. Los GRANTS se heredan hacia abajo:
# MAGIC si das `SELECT` a nivel catalog, aplica a **todas** las tablas presentes y futuras.
# MAGIC
# MAGIC En este lab vamos a crear un catalog de ejemplo con schemas bronze/silver/gold
# MAGIC (la arquitectura medallon clasica).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear catalog y schemas
# MAGIC
# MAGIC Arrancamos creando la estructura. Usamos un catalog con prefijo `lab_` para no pisar nada existente.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear el catalog del lab
# MAGIC CREATE CATALOG IF NOT EXISTS lab_unity_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear los schemas de la arquitectura medallon
# MAGIC CREATE SCHEMA IF NOT EXISTS lab_unity_catalog.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS lab_unity_catalog.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS lab_unity_catalog.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tambien creamos un schema para funciones de seguridad
# MAGIC CREATE SCHEMA IF NOT EXISTS lab_unity_catalog.security;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que se crearon correctamente
# MAGIC SHOW SCHEMAS IN lab_unity_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear tablas de ejemplo
# MAGIC
# MAGIC Necesitamos datos para practicar. Vamos a crear una tabla de clientes con datos
# MAGIC sensibles (email, SSN, region) que despues vamos a proteger.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla bronze: datos crudos de clientes
# MAGIC CREATE OR REPLACE TABLE lab_unity_catalog.bronze.raw_customers (
# MAGIC   customer_id INT,
# MAGIC   nombre STRING,
# MAGIC   email STRING,
# MAGIC   ssn STRING,
# MAGIC   region STRING,
# MAGIC   monto_total DECIMAL(10, 2),
# MAGIC   fecha_registro DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO lab_unity_catalog.bronze.raw_customers VALUES
# MAGIC   (1, 'Maria Garcia', 'maria.garcia@empresa.com', '123-45-6789', 'LATAM', 15000.50, '2024-01-15'),
# MAGIC   (2, 'John Smith', 'john.smith@company.com', '987-65-4321', 'US', 8200.00, '2024-02-20'),
# MAGIC   (3, 'Hans Mueller', 'hans.mueller@firma.de', '456-78-9012', 'EU', 12300.75, '2024-03-10'),
# MAGIC   (4, 'Ana Rodriguez', 'ana.rodriguez@correo.uy', '321-54-6789', 'LATAM', 9800.25, '2024-04-05'),
# MAGIC   (5, 'Pierre Dupont', 'pierre.dupont@societe.fr', '654-32-1098', 'EU', 18500.00, '2024-05-12'),
# MAGIC   (6, 'Laura Martinez', 'laura.martinez@mail.ar', '789-01-2345', 'LATAM', 7600.80, '2024-06-01'),
# MAGIC   (7, 'James Wilson', 'james.wilson@corp.us', '234-56-7890', 'US', 22000.00, '2024-07-18'),
# MAGIC   (8, 'Sofia Pereira', 'sofia.pereira@empresa.br', '567-89-0123', 'LATAM', 11400.60, '2024-08-22');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar los datos
# MAGIC SELECT * FROM lab_unity_catalog.bronze.raw_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. GRANTS: permisos y herencia
# MAGIC
# MAGIC Los GRANTS en Unity Catalog se heredan hacia abajo en la jerarquia.
# MAGIC Esto es comodo pero peligroso si no se controla.
# MAGIC
# MAGIC **Mal ejemplo** -- dar SELECT a nivel catalog:
# MAGIC ```sql
# MAGIC GRANT SELECT ON CATALOG prod TO `data-analysts`;
# MAGIC -- Esto da acceso a TODAS las tablas, incluso las que crees en el futuro
# MAGIC ```
# MAGIC
# MAGIC **Buen ejemplo** -- dar SELECT solo al schema gold:
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG prod TO `data-analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod.gold TO `data-analysts`;
# MAGIC GRANT SELECT ON SCHEMA prod.gold TO `data-analysts`;
# MAGIC ```
# MAGIC
# MAGIC La regla: siempre da permisos al nivel mas bajo posible.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- En Free Edition somos admin, asi que los GRANTS no van a restringirnos.
# MAGIC -- Pero la sintaxis es lo que importa para aprender el patron.
# MAGIC
# MAGIC -- Patron correcto: permisos granulares por schema
# MAGIC -- (en un workspace real, reemplaza `account users` por tu grupo)
# MAGIC
# MAGIC -- Paso 1: permitir "ver" el catalog (sin esto no pueden navegar)
# MAGIC GRANT USE CATALOG ON CATALOG lab_unity_catalog TO `account users`;
# MAGIC
# MAGIC -- Paso 2: permitir "ver" solo el schema gold
# MAGIC GRANT USE SCHEMA ON SCHEMA lab_unity_catalog.gold TO `account users`;
# MAGIC
# MAGIC -- Paso 3: permitir leer tablas del schema gold
# MAGIC GRANT SELECT ON SCHEMA lab_unity_catalog.gold TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver los grants aplicados al catalog
# MAGIC SHOW GRANTS ON CATALOG lab_unity_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver los grants aplicados al schema gold
# MAGIC SHOW GRANTS ON SCHEMA lab_unity_catalog.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVOCAR permisos: la sintaxis inversa
# MAGIC -- Revocamos lo que dimos para dejarlo limpio
# MAGIC REVOKE SELECT ON SCHEMA lab_unity_catalog.gold FROM `account users`;
# MAGIC REVOKE USE SCHEMA ON SCHEMA lab_unity_catalog.gold FROM `account users`;
# MAGIC REVOKE USE CATALOG ON CATALOG lab_unity_catalog FROM `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que se revocaron
# MAGIC SHOW GRANTS ON CATALOG lab_unity_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ### El error clasico: GRANT a nivel catalog
# MAGIC
# MAGIC Muchos equipos hacen esto por comodidad y despues no saben como restringir:
# MAGIC
# MAGIC ```sql
# MAGIC -- NO HAGAS ESTO en produccion:
# MAGIC GRANT SELECT ON CATALOG prod TO `data-analysts`;
# MAGIC ```
# MAGIC
# MAGIC Con esa sola linea, el grupo `data-analysts` puede leer **toda** tabla que exista
# MAGIC o que se cree en el futuro dentro del catalog. Incluyendo tablas bronze con PII
# MAGIC que todavia no limpiaste.
# MAGIC
# MAGIC La solucion es siempre dar acceso a nivel de schema o tabla individual.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Row-Level Security (filtrado por filas)
# MAGIC
# MAGIC Row-level security te permite que distintos usuarios/grupos vean
# MAGIC distintas filas de la misma tabla, sin cambiar el SQL del consumidor.
# MAGIC
# MAGIC El mecanismo: creas una funcion que retorna `BOOLEAN` y la aplicas como filtro.
# MAGIC Unity Catalog evalua la funcion automaticamente en cada query.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear la funcion de filtrado por region
# MAGIC -- En un caso real, usarias is_member() para chequear grupos.
# MAGIC -- En Free Edition somos admin, asi que usamos is_account_group_member()
# MAGIC -- o simulamos la logica.
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION lab_unity_catalog.security.region_filter(region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('admins') THEN true
# MAGIC   WHEN is_account_group_member('latam-team') THEN region IN ('LATAM', 'BR', 'UY', 'AR')
# MAGIC   WHEN is_account_group_member('eu-team') THEN region IN ('EU', 'UK', 'DE', 'FR')
# MAGIC   ELSE true  -- en Free Edition dejamos pasar todo para poder ver el resultado
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicar el row filter a la tabla bronze
# MAGIC ALTER TABLE lab_unity_catalog.bronze.raw_customers
# MAGIC SET ROW FILTER lab_unity_catalog.security.region_filter ON (region);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ahora al hacer SELECT, el filtro se aplica automaticamente.
# MAGIC -- Como somos admin, vemos todas las filas.
# MAGIC -- Un usuario del grupo 'latam-team' veria solo LATAM.
# MAGIC SELECT * FROM lab_unity_catalog.bronze.raw_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que el filtro esta aplicado
# MAGIC DESCRIBE EXTENDED lab_unity_catalog.bronze.raw_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remover el row filter (lo vamos a necesitar limpio para el proximo ejercicio)
# MAGIC ALTER TABLE lab_unity_catalog.bronze.raw_customers
# MAGIC DROP ROW FILTER;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Column Masking (enmascaramiento de columnas)
# MAGIC
# MAGIC Column masking permite ocultar o transformar datos sensibles (PII) para usuarios
# MAGIC que no tienen autorizacion. El dato original sigue existiendo, pero el usuario
# MAGIC solo ve la version enmascarada.
# MAGIC
# MAGIC Casos tipicos: emails, SSN, numeros de tarjeta, telefonos.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Funcion de masking para emails
# MAGIC -- Convierte "maria.garcia@empresa.com" en "m***@empresa.com"
# MAGIC CREATE OR REPLACE FUNCTION lab_unity_catalog.security.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('pii-authorized') THEN email
# MAGIC   ELSE regexp_replace(email, '(.).*@', '$1***@')
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Funcion de masking para SSN
# MAGIC -- Convierte "123-45-6789" en "***-**-6789" (muestra solo ultimos 4)
# MAGIC CREATE OR REPLACE FUNCTION lab_unity_catalog.security.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('pii-authorized') THEN ssn
# MAGIC   ELSE concat('***-**-', substring(ssn, 8, 4))
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicar las mascaras a las columnas de la tabla
# MAGIC ALTER TABLE lab_unity_catalog.bronze.raw_customers
# MAGIC ALTER COLUMN email SET MASK lab_unity_catalog.security.mask_email;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lab_unity_catalog.bronze.raw_customers
# MAGIC ALTER COLUMN ssn SET MASK lab_unity_catalog.security.mask_ssn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consultar la tabla: como no somos parte del grupo 'pii-authorized',
# MAGIC -- vemos los datos enmascarados.
# MAGIC -- (en Free Edition, depende de tu configuracion de grupos)
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   nombre,
# MAGIC   email,
# MAGIC   ssn,
# MAGIC   region
# MAGIC FROM lab_unity_catalog.bronze.raw_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar las mascaras aplicadas
# MAGIC DESCRIBE EXTENDED lab_unity_catalog.bronze.raw_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remover las mascaras
# MAGIC ALTER TABLE lab_unity_catalog.bronze.raw_customers
# MAGIC ALTER COLUMN email DROP MASK;
# MAGIC
# MAGIC ALTER TABLE lab_unity_catalog.bronze.raw_customers
# MAGIC ALTER COLUMN ssn DROP MASK;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Volumes: archivos no tabulares
# MAGIC
# MAGIC Desde DBR 13.3, Unity Catalog gobierna tambien archivos: CSVs, JSONs, modelos ML, artefactos.
# MAGIC Los Volumes son como "carpetas gobernadas" con los mismos GRANTS que las tablas.
# MAGIC
# MAGIC Hay dos tipos:
# MAGIC - **Managed**: Databricks gestiona el storage
# MAGIC - **External**: vos controlas el storage (S3, ADLS, GCS)
# MAGIC
# MAGIC En Free Edition solo podemos usar managed volumes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear un volume managed
# MAGIC CREATE VOLUME IF NOT EXISTS lab_unity_catalog.bronze.landing_zone;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que se creo
# MAGIC SHOW VOLUMES IN lab_unity_catalog.bronze;

# COMMAND ----------

# Escribir un archivo CSV de ejemplo en el volume
import csv

volume_path = "/Volumes/lab_unity_catalog/bronze/landing_zone"

# Crear un CSV de ejemplo con datos de productos
data = [
    ["product_id", "nombre", "categoria", "precio"],
    [1, "Notebook Lenovo", "Tecnologia", 850.00],
    [2, "Monitor 27 pulgadas", "Tecnologia", 420.00],
    [3, "Silla Ergonomica", "Oficina", 350.00],
    [4, "Teclado Mecanico", "Tecnologia", 120.00],
    [5, "Escritorio Ajustable", "Oficina", 680.00],
]

output_file = f"{volume_path}/productos.csv"

dbutils.fs.put(
    output_file,
    "\n".join([",".join(str(c) for c in row) for row in data]),
    overwrite=True
)

print(f"Archivo creado en: {output_file}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Listar archivos en el volume
# MAGIC LIST '/Volumes/lab_unity_catalog/bronze/landing_zone/';

# COMMAND ----------

# Leer el CSV desde el volume usando Spark
df_productos = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/lab_unity_catalog/bronze/landing_zone/productos.csv")
)

df_productos.show()

# COMMAND ----------

# MAGIC %md
# MAGIC El archivo queda gobernado por Unity Catalog: tiene permisos, linaje y auditoria.
# MAGIC Los grants para volumes son `READ VOLUME` y `WRITE VOLUME`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Linaje automatico
# MAGIC
# MAGIC Unity Catalog trackea automaticamente el linaje de datos para cualquier operacion
# MAGIC que pase por Spark. Cada vez que lees de una tabla y escribis en otra,
# MAGIC el linaje queda registrado.
# MAGIC
# MAGIC Vamos a crear tablas silver y gold derivadas de bronze para generar linaje.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver: limpiar y transformar datos de clientes
# MAGIC CREATE OR REPLACE TABLE lab_unity_catalog.silver.customers AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   nombre,
# MAGIC   lower(email) AS email,
# MAGIC   region,
# MAGIC   monto_total,
# MAGIC   fecha_registro,
# MAGIC   current_timestamp() AS processed_at
# MAGIC FROM lab_unity_catalog.bronze.raw_customers
# MAGIC WHERE customer_id IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold: agregado por region para consumo de negocio
# MAGIC CREATE OR REPLACE TABLE lab_unity_catalog.gold.customers_by_region AS
# MAGIC SELECT
# MAGIC   region,
# MAGIC   count(*) AS total_customers,
# MAGIC   sum(monto_total) AS revenue_total,
# MAGIC   avg(monto_total) AS revenue_promedio,
# MAGIC   min(fecha_registro) AS primera_alta,
# MAGIC   max(fecha_registro) AS ultima_alta
# MAGIC FROM lab_unity_catalog.silver.customers
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar la tabla gold
# MAGIC SELECT * FROM lab_unity_catalog.gold.customers_by_region
# MAGIC ORDER BY revenue_total DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ver el linaje
# MAGIC
# MAGIC Ahora en el Catalog Explorer de Databricks, si navegas a la tabla
# MAGIC `lab_unity_catalog.gold.customers_by_region` y vas a la pestana **Lineage**,
# MAGIC vas a ver el grafo:
# MAGIC
# MAGIC ```
# MAGIC bronze.raw_customers --> silver.customers --> gold.customers_by_region
# MAGIC ```
# MAGIC
# MAGIC Esto se genera automaticamente, sin configurar nada.
# MAGIC
# MAGIC Para que el linaje sea completo:
# MAGIC 1. Usa siempre tablas de Unity Catalog (no paths directos a S3/ADLS)
# MAGIC 2. Evita `df.write.parquet()` -- usa `df.write.saveAsTable()` o CTAS
# MAGIC 3. En DLT, el linaje es automatico al 100%

# COMMAND ----------

# Tambien podemos crear linaje desde PySpark
df_silver = spark.table("lab_unity_catalog.silver.customers")

df_top_customers = (
    df_silver
    .filter("monto_total > 10000")
    .select("customer_id", "nombre", "region", "monto_total")
    .orderBy("monto_total", ascending=False)
)

# Esto genera linaje: silver.customers -> gold.top_customers
(
    df_top_customers
    .write
    .mode("overwrite")
    .saveAsTable("lab_unity_catalog.gold.top_customers")
)

print("Tabla gold.top_customers creada con linaje automatico")
spark.table("lab_unity_catalog.gold.top_customers").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumen de lo que vimos
# MAGIC
# MAGIC | Concepto | Que hace | Comando clave |
# MAGIC |----------|----------|---------------|
# MAGIC | Catalog/Schema | Organiza datos en 3 niveles | `CREATE CATALOG`, `CREATE SCHEMA` |
# MAGIC | GRANTS | Controla quien accede a que | `GRANT SELECT ON SCHEMA ... TO ...` |
# MAGIC | Row Filter | Filtra filas por usuario/grupo | `ALTER TABLE ... SET ROW FILTER` |
# MAGIC | Column Mask | Enmascara columnas sensibles | `ALTER TABLE ... ALTER COLUMN ... SET MASK` |
# MAGIC | Volumes | Gobierna archivos no tabulares | `CREATE VOLUME`, `/Volumes/...` |
# MAGIC | Lineage | Trackea origen de los datos | Automatico con tablas UC |
# MAGIC
# MAGIC La regla mas importante: **da permisos al nivel mas bajo posible**.
# MAGIC No des `SELECT ON CATALOG` salvo que realmente necesites que todos lean todo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cleanup
# MAGIC
# MAGIC Ejecuta esta celda para borrar todo lo que creamos en el lab.
# MAGIC Si queres conservar algo, comenta las lineas correspondientes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Borrar tablas gold
# MAGIC DROP TABLE IF EXISTS lab_unity_catalog.gold.customers_by_region;
# MAGIC DROP TABLE IF EXISTS lab_unity_catalog.gold.top_customers;
# MAGIC
# MAGIC -- Borrar tabla silver
# MAGIC DROP TABLE IF EXISTS lab_unity_catalog.silver.customers;
# MAGIC
# MAGIC -- Borrar tabla bronze
# MAGIC DROP TABLE IF EXISTS lab_unity_catalog.bronze.raw_customers;
# MAGIC
# MAGIC -- Borrar funciones de seguridad
# MAGIC DROP FUNCTION IF EXISTS lab_unity_catalog.security.region_filter;
# MAGIC DROP FUNCTION IF EXISTS lab_unity_catalog.security.mask_email;
# MAGIC DROP FUNCTION IF EXISTS lab_unity_catalog.security.mask_ssn;
# MAGIC
# MAGIC -- Borrar volume (esto borra tambien los archivos adentro)
# MAGIC DROP VOLUME IF EXISTS lab_unity_catalog.bronze.landing_zone;
# MAGIC
# MAGIC -- Borrar schemas
# MAGIC DROP SCHEMA IF EXISTS lab_unity_catalog.security CASCADE;
# MAGIC DROP SCHEMA IF EXISTS lab_unity_catalog.gold CASCADE;
# MAGIC DROP SCHEMA IF EXISTS lab_unity_catalog.silver CASCADE;
# MAGIC DROP SCHEMA IF EXISTS lab_unity_catalog.bronze CASCADE;
# MAGIC
# MAGIC -- Borrar catalog
# MAGIC DROP CATALOG IF EXISTS lab_unity_catalog CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que se limpio todo
# MAGIC SHOW CATALOGS LIKE 'lab_unity*';
