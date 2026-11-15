# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text(
    "ENVIRONMENT",
    "dev",
    "Environment: dev | prod"
)

# COMMAND ----------

# ============================================================
# ANDINASTORE ANALYTICS
# PREAMBULO - CONFIGURACION DEL AMBIENTE
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

print("=" * 70)
print("ANDINASTORE - CONFIGURACION DE AMBIENTE")
print("=" * 70)

print(f"Ambiente : {ENVIRONMENT.upper()}")
print(f"Catalogo : {CATALOG}")

print("\nSchemas:")
print(f"Bronze   : {BRONZE_SCHEMA}")
print(f"Silver   : {SILVER_SCHEMA}")
print(f"Gold     : {GOLD_SCHEMA}")

# COMMAND ----------

# ============================================================
# PREPARACION DEL CATALOGO
# ============================================================

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG}
""")

print(
    f"[OK] Catalogo disponible: {CATALOG}"
)


# COMMAND ----------

# ============================================================
# PREPARACION DE SCHEMAS MEDALLION
# ============================================================

SCHEMAS = [
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA
]

for schema in SCHEMAS:

    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {schema}
    """)

    print(
        f"[OK] Schema disponible: {schema}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # TABLAS BRONCE

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.customers (
    customer_id STRING,
    full_name STRING,
    email STRING,
    city STRING,
    department STRING,
    registration_date STRING,
    customer_segment STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.products (
    product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    price STRING,
    cost STRING,
    active STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.sales (
    sale_id STRING,
    customer_id STRING,
    product_id STRING,
    sale_date STRING,
    quantity STRING,
    discount_pct STRING,
    payment_method STRING,
    channel STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC > ## TABLES DE SILVER

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.customers (
    customer_id STRING,
    full_name STRING,
    email STRING,
    city STRING,
    department STRING,
    registration_date DATE,
    customer_segment STRING,
    _processed_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.products (
    product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    price DECIMAL(12,2),
    cost DECIMAL(12,2),
    active BOOLEAN,
    _processed_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.sales (
    sale_id STRING,
    customer_id STRING,
    product_id STRING,
    sale_date DATE,
    quantity INT,
    discount_pct DECIMAL(5,2),
    payment_method STRING,
    channel STRING,
    _processed_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.rejected_sales (
    sale_id STRING,
    customer_id STRING,
    product_id STRING,
    sale_date STRING,
    quantity STRING,
    discount_pct STRING,
    payment_method STRING,
    channel STRING,
    rejection_reason STRING,
    _rejected_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

display(
    spark.sql(f"""
        SHOW TABLES IN {SILVER_SCHEMA}
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # TABLES GOLD

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.dim_customer (
    customer_id STRING,
    full_name STRING,
    email STRING,
    city STRING,
    department STRING,
    registration_date DATE,
    customer_segment STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.dim_product (
    product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    price DECIMAL(12,2),
    cost DECIMAL(12,2),
    active BOOLEAN
)
USING DELTA
""")


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.dim_date (
    date_id INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day INT,
    day_of_week INT,
    day_name STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.fact_sales (
    sale_id STRING,
    customer_id STRING,
    product_id STRING,
    date_id INT,
    quantity INT,
    unit_price DECIMAL(12,2),
    unit_cost DECIMAL(12,2),
    discount_pct DECIMAL(5,2),
    gross_amount DECIMAL(14,2),
    discount_amount DECIMAL(14,2),
    net_sales DECIMAL(14,2),
    total_cost DECIMAL(14,2),
    profit DECIMAL(14,2),
    payment_method STRING,
    channel STRING
)
USING DELTA
""")