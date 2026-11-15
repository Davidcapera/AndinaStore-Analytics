# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T
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

from pyspark.sql import functions as F

df_customers_silver = spark.table(
    f"{SILVER_SCHEMA}.customers"
)

df_products_silver = spark.table(
    f"{SILVER_SCHEMA}.products"
)

df_sales_silver = spark.table(
    f"{SILVER_SCHEMA}.sales"
)
df_dim_customer_gold = spark.table(
    f"{GOLD_SCHEMA}.dim_customer"
)

df_dim_product_gold = spark.table(
    f"{GOLD_SCHEMA}.dim_product"
)

df_dim_date_gold = spark.table(
    f"{GOLD_SCHEMA}.dim_date"
)
print("===== FUENTES SILVER =====")
print(f"Customers: {df_customers_silver.count():,}")
print(f"Products : {df_products_silver.count():,}")
print(f"Sales    : {df_sales_silver.count():,}")


# COMMAND ----------

# MAGIC %md
# MAGIC CUSTOMER

# COMMAND ----------

df_dim_customer = (
    df_customers_silver
    .select(
        "customer_id",
        "full_name",
        "email",
        "city",
        "department",
        "registration_date",
        "customer_segment"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **PRODUCT**

# COMMAND ----------

df_dim_product = (
    df_products_silver
    .select(
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "active"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDACIONES ANTES DE CARGA

# COMMAND ----------

# MAGIC %md
# MAGIC **CUSTOMER**

# COMMAND ----------

total_dim_customer = df_dim_customer.count()

unique_customer_ids = (
    df_dim_customer
    .select("customer_id")
    .distinct()
    .count()
)

null_customer_ids = (
    df_dim_customer
    .filter(F.col("customer_id").isNull())
    .count()
)

print("===== VALIDACION DIM_CUSTOMER =====")
print(f"Registros totales     : {total_dim_customer:,}")
print(f"Customer ID únicos    : {unique_customer_ids:,}")
print(f"Customer ID nulos     : {null_customer_ids:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **PRODUCT**

# COMMAND ----------

total_dim_product = df_dim_product.count()

unique_product_ids = (
    df_dim_product
    .select("product_id")
    .distinct()
    .count()
)

null_product_ids = (
    df_dim_product
    .filter(F.col("product_id").isNull())
    .count()
)

print("===== VALIDACION DIM_PRODUCT =====")
print(f"Registros totales  : {total_dim_product:,}")
print(f"Product ID únicos  : {unique_product_ids:,}")
print(f"Product ID nulos   : {null_product_ids:,}")


# COMMAND ----------

invalid_financial_values = (
    df_dim_product
    .filter(
        (F.col("price") <= 0) |
        (F.col("cost") <= 0)
    )
    .count()
)

print(
    "Productos con precio/costo invalido:",
    invalid_financial_values
)

assert invalid_financial_values == 0, \
    "dim_product contiene precios o costos invalidos"

print(" Valores financieros validos")

# COMMAND ----------

# MAGIC %md
# MAGIC **FECHA**

# COMMAND ----------

rango_fechas = (
    df_sales_silver
    .agg(
        F.min("sale_date").alias("fecha_minima"),
        F.max("sale_date").alias("fecha_maxima")
    )
)

display(rango_fechas)

# COMMAND ----------

fechas = rango_fechas.first()

fecha_minima = fechas["fecha_minima"]
fecha_maxima = fechas["fecha_maxima"]

print("Fecha minima:", fecha_minima)
print("Fecha maxima:", fecha_maxima)

# COMMAND ----------

df_dim_date = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                DATE('{fecha_minima}'),
                DATE('{fecha_maxima}'),
                INTERVAL 1 DAY
            )
        ) AS full_date
    """)

    .withColumn(
        "date_id",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )

    .withColumn(
        "year",
        F.year("full_date")
    )

    .withColumn(
        "quarter",
        F.quarter("full_date")
    )

    .withColumn(
        "month",
        F.month("full_date")
    )

    .withColumn(
        "month_name",
        F.date_format("full_date", "MMMM")
    )

    .withColumn(
        "day",
        F.dayofmonth("full_date")
    )

    .withColumn(
        "day_of_week",
        F.dayofweek("full_date")
    )

    .withColumn(
        "day_name",
        F.date_format("full_date", "EEEE")
    )

    .select(
        "date_id",
        "full_date",
        "year",
        "quarter",
        "month",
        "month_name",
        "day",
        "day_of_week",
        "day_name"
    )
)

# COMMAND ----------

total_dates = df_dim_date.count()

unique_date_ids = (
    df_dim_date
    .select("date_id")
    .distinct()
    .count()
)

null_date_ids = (
    df_dim_date
    .filter(F.col("date_id").isNull())
    .count()
)

print("===== VALIDACION DIM_DATE =====")
print(f"Fechas generadas : {total_dates:,}")
print(f"Date ID Unicos   : {unique_date_ids:,}")
print(f"Date ID nulos    : {null_date_ids:,}")

# COMMAND ----------

display(
    df_dim_date
    .orderBy("full_date")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **SALES**

# COMMAND ----------

orphan_customers = (
    df_sales_silver
    .join(
        df_dim_customer_gold.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
)

print(
    "Ventas con cliente inexistente:",
    orphan_customers.count()
)

# COMMAND ----------

orphan_products = (
    df_sales_silver
    .join(
        df_dim_product_gold.select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

print(
    "Ventas con producto inexistente:",
    orphan_products.count()
)


# COMMAND ----------

orphan_dates = (
    df_sales_silver
    .join(
        df_dim_date_gold.select(
            F.col("full_date").alias("sale_date")
        ),
        on="sale_date",
        how="left_anti"
    )
)

print(
    "Ventas con fecha inexistente:",
    orphan_dates.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # CONSTRUCCION DE FACT SALES

# COMMAND ----------

df_fact_sales = (
    df_sales_silver.alias("s")

    .join(
        df_dim_customer_gold.alias("c"),
        F.col("s.customer_id") == F.col("c.customer_id"),
        "inner"
    )

    .join(
        df_dim_product_gold.alias("p"),
        F.col("s.product_id") == F.col("p.product_id"),
        "inner"
    )

    .join(
        df_dim_date_gold.alias("d"),
        F.col("s.sale_date") == F.col("d.full_date"),
        "inner"
    )

    .select(
        F.col("s.sale_id").alias("sale_id"),
        F.col("s.customer_id").alias("customer_id"),
        F.col("s.product_id").alias("product_id"),
        F.col("d.date_id").alias("date_id"),
        F.col("s.quantity").alias("quantity"),
        F.col("p.price").alias("unit_price"),
        F.col("p.cost").alias("unit_cost"),
        F.col("s.discount_pct").alias("discount_pct"),
        F.col("s.payment_method").alias("payment_method"),
        F.col("s.channel").alias("channel")
    )
)

# COMMAND ----------

print(
    "Ventas Silver:",
    df_sales_silver.count()
)

print(
    "Registros Fact despues de joins:",
    df_fact_sales.count()
)

# COMMAND ----------

df_fact_sales = (
    df_fact_sales

    .withColumn(
        "gross_amount",
        F.col("quantity") * F.col("unit_price")
    )

    .withColumn(
        "discount_amount",
        F.col("gross_amount") * F.col("discount_pct")
    )

    .withColumn(
        "net_sales",
        F.col("gross_amount") - F.col("discount_amount")
    )

    .withColumn(
        "total_cost",
        F.col("quantity") * F.col("unit_cost")
    )

    .withColumn(
        "profit",
        F.col("net_sales") - F.col("total_cost")
    )
)

# COMMAND ----------

df_fact_sales = df_fact_sales.select(
    "sale_id",
    "customer_id",
    "product_id",
    "date_id",
    "quantity",
    F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
    F.col("unit_cost").cast("decimal(12,2)").alias("unit_cost"),
    F.col("discount_pct").cast("decimal(5,2)").alias("discount_pct"),
    F.col("gross_amount").cast("decimal(14,2)").alias("gross_amount"),
    F.col("discount_amount").cast("decimal(14,2)").alias("discount_amount"),
    F.col("net_sales").cast("decimal(14,2)").alias("net_sales"),
    F.col("total_cost").cast("decimal(14,2)").alias("total_cost"),
    F.col("profit").cast("decimal(14,2)").alias("profit"),
    "payment_method",
    "channel"
)

# COMMAND ----------

total_fact = df_fact_sales.count()

unique_sales = (
    df_fact_sales
    .select("sale_id")
    .distinct()
    .count()
)

null_keys = (
    df_fact_sales
    .filter(
        F.col("sale_id").isNull() |
        F.col("customer_id").isNull() |
        F.col("product_id").isNull() |
        F.col("date_id").isNull()
    )
    .count()
)

print("===== VALIDACION FACT_SALES =====")
print(f"Registros fact       : {total_fact:,}")
print(f"Sale ID únicos       : {unique_sales:,}")
print(f"Claves nulas         : {null_keys:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC # CARGA GOLD

# COMMAND ----------

# MAGIC %md
# MAGIC **CUSTOMER**

# COMMAND ----------

(
    df_dim_customer.write
    .mode("overwrite")
    .insertInto(f"{GOLD_SCHEMA}.dim_customer")
)

print("gold.dim_customer cargada")

# COMMAND ----------

df_dim_customer_gold = spark.table(
    f"{GOLD_SCHEMA}.dim_customer"
)

print(
    "Registros Gold dim_customer:",
    df_dim_customer_gold.count()
)

display(
    df_dim_customer_gold
    .orderBy("customer_id")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **PRODUCT**

# COMMAND ----------

(
    df_dim_product.write
    .mode("overwrite")
    .insertInto(
        f"{GOLD_SCHEMA}.dim_product"
    )
)

print("gold.dim_product cargada")

# COMMAND ----------

df_dim_product_gold = spark.table(
    f"{GOLD_SCHEMA}.dim_product"
)

print(
    "Registros Gold dim_product:",
    df_dim_product_gold.count()
)

display(
    df_dim_product_gold
    .orderBy("product_id")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **DIM_DATE**

# COMMAND ----------

(
    df_dim_date.write
    .mode("overwrite")
    .insertInto(
        f"{GOLD_SCHEMA}.dim_date"
    )
)

print("gold.dim_date cargada")

# COMMAND ----------

# MAGIC %md
# MAGIC **FAC SALES**

# COMMAND ----------

(
    df_fact_sales.write
    .mode("overwrite")
    .insertInto(
        f"{GOLD_SCHEMA}.fact_sales"
    )
)

print("gold.fact_sales cargada correctamente")

# COMMAND ----------

df_fact_gold = spark.table(
    f"{GOLD_SCHEMA}.fact_sales"
)

print("===== VALIDACIÓN GOLD FACT_SALES =====")
print(f"Registros: {df_fact_gold.count():,}")

display(
    df_fact_gold
    .orderBy("sale_id")
    .limit(20)
)

# COMMAND ----------

resumen_gold = (
    df_fact_gold
    .agg(
        F.count("*").alias("total_sales"),
        F.sum("quantity").alias("units_sold"),
        F.sum("gross_amount").alias("gross_amount"),
        F.sum("discount_amount").alias("discount_amount"),
        F.sum("net_sales").alias("net_sales"),
        F.sum("total_cost").alias("total_cost"),
        F.sum("profit").alias("profit")
    )
)

display(resumen_gold)


# COMMAND ----------

gold_tables = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales"
]

print("===== MODELO GOLD =====")

for table in gold_tables:

    count = spark.table(
        f"{GOLD_SCHEMA}.{table}"
    ).count()

    print(f"{table}: {count:,}")