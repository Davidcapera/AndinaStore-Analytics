# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # imports y configuracion

# COMMAND ----------

from pyspark.sql import functions as F
dbutils.widgets.text(
    "ENVIRONMENT",
    "dev",
    "Environment: dev | prod"
)

# COMMAND ----------

# ============================================================
# CONFIGURACION DEL AMBIENTE
# ============================================================

ENVIRONMENT = dbutils.widgets.get("ENVIRONMENT").strip().lower()

VALID_ENVIRONMENTS = [
    "dev",
    "prod"
]

if ENVIRONMENT not in VALID_ENVIRONMENTS:
    raise ValueError(
        f"ENVIRONMENT invalido: '{ENVIRONMENT}'. "
        f"Valores permitidos: {VALID_ENVIRONMENTS}"
    )

CATALOG = f"andinastore_{ENVIRONMENT}"

BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

print(f"Ambiente: {ENVIRONMENT.upper()}")
print(f"Catalogo: {CATALOG}")


# COMMAND ----------

STORAGE_ACCOUNT = "standinastoredata"

RAW_BASE_PATH = (
    f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
)

CUSTOMERS_PATH = f"{RAW_BASE_PATH}/customers.csv"
PRODUCTS_PATH = f"{RAW_BASE_PATH}/products.csv"
SALES_PATH = f"{RAW_BASE_PATH}/sales.csv"

print("RAW:", RAW_BASE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ****Función** de extracción**

# COMMAND ----------

def extract_csv(path):
    """
    Lee un archivo CSV desde RAW conservando
    inicialmente sus columnas como STRING.
    """
    
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .csv(path)
    )

# COMMAND ----------

df_customers_raw = extract_csv(CUSTOMERS_PATH)
df_products_raw = extract_csv(PRODUCTS_PATH)
df_sales_raw = extract_csv(SALES_PATH)

print(" Extraccion RAW completada")

# COMMAND ----------

def add_ingestion_metadata(df):
    return (
        df
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path")
        )
    )

# COMMAND ----------

df_customers_bronze = add_ingestion_metadata(df_customers_raw)
df_products_bronze = add_ingestion_metadata(df_products_raw)
df_sales_bronze = add_ingestion_metadata(df_sales_raw)

# COMMAND ----------

raw_counts = {
    "customers": df_customers_bronze.count(),
    "products": df_products_bronze.count(),
    "sales": df_sales_bronze.count()
}

for dataset, rows in raw_counts.items():
    print(f"{dataset}: {rows:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC # INGESTA A BRONCE

# COMMAND ----------

df_customers_bronze.write \
    .mode("overwrite") \
    .insertInto(f"{BRONZE_SCHEMA}.customers")

df_products_bronze.write \
    .mode("overwrite") \
    .insertInto(f"{BRONZE_SCHEMA}.products")

df_sales_bronze.write \
    .mode("overwrite") \
    .insertInto(f"{BRONZE_SCHEMA}.sales")

print(f" Carga {BRONZE_SCHEMA} completada")

# COMMAND ----------

# MAGIC %md
# MAGIC **VALDIAR **RESULTADOS****

# COMMAND ----------

bronze_tables = [
    f"{BRONZE_SCHEMA}.customers",
    f"{BRONZE_SCHEMA}.products",
    f"{BRONZE_SCHEMA}.sales"
]

print("===== VALIDACION BRONZE =====")

for table in bronze_tables:
    count = spark.table(table).count()
    print(f"{table}: {count:,} registros")

# COMMAND ----------

display(
    spark.table(f"{BRONZE_SCHEMA}.sales").limit(10)
)