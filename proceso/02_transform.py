# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # imports y configuracion

# COMMAND ----------

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


df_customers_bronze = spark.table(
    f"{BRONZE_SCHEMA}.customers"
)

df_products_bronze = spark.table(
    f"{BRONZE_SCHEMA}.products"
)

df_sales_bronze = spark.table(
    f"{BRONZE_SCHEMA}.sales"
)

print("Customers Bronze:", df_customers_bronze.count())
print("Products Bronze :", df_products_bronze.count())
print("Sales Bronze    :", df_sales_bronze.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDACIONES

# COMMAND ----------

duplicados_customers = (
    df_customers_bronze
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

print("IDs de clientes duplicados:")
display(duplicados_customers)

# COMMAND ----------

emails_vacios = df_customers_bronze.filter(
    F.col("email").isNull() |
    (F.trim(F.col("email")) == "")
)

print("Clientes con email vacío:", emails_vacios.count())

# COMMAND ----------

departamentos_vacios = df_customers_bronze.filter(
    F.col("department").isNull() |
    (F.trim(F.col("department")) == "")
)

print("Clientes con departamento vacio:", departamentos_vacios.count())

# COMMAND ----------

(
    df_customers_bronze
    .groupBy("city")
    .count()
    .orderBy("city")
    .show(truncate=False)
)

# COMMAND ----------

display(df_customers_bronze)

# COMMAND ----------

duplicados_products = (
    df_products_bronze
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

print("IDs de productos duplicados:")
display(duplicados_products)

# COMMAND ----------

marcas_vacias = df_products_bronze.filter(
    F.col("brand").isNull() |
    (F.trim(F.col("brand")) == "")
)

print("Marcas vacías:", marcas_vacias.count())

# COMMAND ----------

productos_invalidos = df_products_bronze.filter(
    (F.col("price").cast("decimal(12,2)") <= 0) |
    (F.col("cost").cast("decimal(12,2)") <= 0)
)

print("Precio/costo invalido:", productos_invalidos.count())

display(productos_invalidos)

# COMMAND ----------

# MAGIC %md
# MAGIC # TRANSFORMACION

# COMMAND ----------

# MAGIC %md
# MAGIC **CUSTOMER**

# COMMAND ----------

df_customers_silver=(
    df_customers_bronze
    # Una fila por customer_id
    .dropDuplicates(["customer_id"])

    # Limpieza basica
    .withColumn(
        "full_name",
        F.initcap(F.trim(F.col("full_name")))
    )

    .withColumn(
        "email",
        F.when(
            F.trim(F.col("email")) == "",
            F.lit(None)
        ).otherwise(
            F.lower(F.trim(F.col("email")))
        )
    )

    .withColumn(
        "city",
        F.initcap(F.lower(F.trim(F.col("city"))))
    )

    .withColumn(
        "department",
        F.when(
            F.trim(F.col("department")) == "",
            F.lit(None)
        ).otherwise(
            F.initcap(F.lower(F.trim(F.col("department"))))
        )
    )

    .withColumn(
        "customer_segment",
        F.initcap(F.lower(F.trim(F.col("customer_segment"))))
    )

    # STRING → DATE
    .withColumn(
        "registration_date",
        F.to_date(
            F.col("registration_date"),
            "yyyy-MM-dd"
        )
    )

    .withColumn(
        "_processed_timestamp",
        F.current_timestamp()
    )

    # Solo columnas Silver
    .select(
        "customer_id",
        "full_name",
        "email",
        "city",
        "department",
        "registration_date",
        "customer_segment",
        "_processed_timestamp"
    )
)

# COMMAND ----------

print(
    "Customers Bronze:",
    df_customers_bronze.count()
)

print(
    "Customers Silver:",
    df_customers_silver.count()
)


# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCTS

# COMMAND ----------

df_products_silver = (
    df_products_bronze
    # Una fila por producto
    .dropDuplicates(["product_id"])
    # Normalizacion
    .withColumn(
        "product_name",
        F.initcap(F.trim(F.col("product_name")))
    )
    .withColumn(
        "category",
        F.initcap(F.lower(F.trim(F.col("category"))))
    )
    .withColumn(
        "brand",
        F.when(
            F.col("brand").isNull() |
            (F.trim(F.col("brand")) == ""),
            F.lit("Unknown")
        ).otherwise(
            F.initcap(F.lower(F.trim(F.col("brand"))))
        )
    )
    # Tipificacion
    .withColumn(
        "price",
        F.col("price").cast("decimal(12,2)")
    )
    .withColumn(
        "cost",
        F.col("cost").cast("decimal(12,2)")
    )
    .withColumn(
        "active",
        F.col("active").cast("boolean")
    )
    # Reglas de negocio
    .filter(F.col("price") > 0)
    .filter(F.col("cost") > 0)
    # Auditoria
    .withColumn(
        "_processed_timestamp",
        F.current_timestamp()
    )
    .select(
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "active",
        "_processed_timestamp"
    )
)

# COMMAND ----------

print(
    "Products Bronze:",
    df_products_bronze.count()
)

print(
    "Products Silver:",
    df_products_silver.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **SALES**

# COMMAND ----------

df_sales_typed = (
    df_sales_bronze
    .dropDuplicates(["sale_id"])

    .withColumn(
        "sale_date_parsed",
        F.expr("try_to_date(sale_date, 'yyyy-MM-dd')")
    )

    .withColumn(
        "quantity_parsed",
        F.expr("try_cast(quantity AS INT)")
    )

    .withColumn(
        "discount_parsed",
        F.expr("try_cast(discount_pct AS DECIMAL(5,2))")
    )
)

# COMMAND ----------

df_sales_validated = (
    df_sales_typed

    .withColumn(
        "_invalid_date",
        F.col("sale_date_parsed").isNull()
    )

    .withColumn(
        "_invalid_quantity",
        F.col("quantity_parsed").isNull() |
        (F.col("quantity_parsed") <= 0)
    )

    .withColumn(
        "_invalid_discount",
        F.col("discount_parsed").isNull() |
        (F.col("discount_parsed") < 0) |
        (F.col("discount_parsed") > 1)
    )
)

# COMMAND ----------

display(
    df_sales_validated)

# COMMAND ----------

valid_customers = (
    df_customers_silver
    .select("customer_id")
    .withColumn("_valid_customer", F.lit(True))
)

valid_products = (
    df_products_silver
    .select("product_id")
    .withColumn("_valid_product", F.lit(True))
)

# COMMAND ----------

df_sales_validated = (
    df_sales_validated

    .join(
        valid_customers,
        on="customer_id",
        how="left"
    )

    .join(
        valid_products,
        on="product_id",
        how="left"
    )

    .withColumn(
        "_invalid_customer",
        F.col("_valid_customer").isNull()
    )

    .withColumn(
        "_invalid_product",
        F.col("_valid_product").isNull()
    )
)

# COMMAND ----------

df_sales_validated = (
    df_sales_validated
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            " | ",

            F.when(
                F.col("_invalid_date"),
                F.lit("INVALID_DATE")
            ),

            F.when(
                F.col("_invalid_quantity"),
                F.lit("INVALID_QUANTITY")
            ),

            F.when(
                F.col("_invalid_discount"),
                F.lit("INVALID_DISCOUNT")
            ),

            F.when(
                F.col("_invalid_customer"),
                F.lit("CUSTOMER_NOT_FOUND")
            ),

            F.when(
                F.col("_invalid_product"),
                F.lit("PRODUCT_NOT_FOUND")
            )
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC REGISTROS-RECHAZADOS

# COMMAND ----------

df_rejected_sales = (
    df_sales_validated

    .filter(
        F.col("rejection_reason") != ""
    )

    .withColumn(
        "_rejected_timestamp",
        F.current_timestamp()
    )

    .select(
        "sale_id",
        "customer_id",
        "product_id",
        "sale_date",
        "quantity",
        "discount_pct",
        "payment_method",
        "channel",
        "rejection_reason",
        "_rejected_timestamp"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC REGISTROS VALIDOS

# COMMAND ----------

df_sales_silver = (
    df_sales_validated

    .filter(
        F.col("rejection_reason") == ""
    )

    .withColumn(
        "_processed_timestamp",
        F.current_timestamp()
    )

    .select(
        "sale_id",
        "customer_id",
        "product_id",
        F.col("sale_date_parsed").alias("sale_date"),
        F.col("quantity_parsed").alias("quantity"),
        F.col("discount_parsed").alias("discount_pct"),
        "payment_method",
        "channel",
        "_processed_timestamp"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC RESULTADO

# COMMAND ----------

print("===== SALES QUALITY =====")

print("Bronze:", df_sales_bronze.count())
print("Después de quitar duplicados:", df_sales_typed.count())
print("Válidos:", df_sales_silver.count())
print("Rechazados:", df_rejected_sales.count())

# COMMAND ----------

assert (
    df_sales_silver.count()
    + df_rejected_sales.count()
    == df_sales_typed.count()
), "ERROR: existen ventas que no quedaron clasificadas"

print(" Todas las ventas únicas fueron clasificadas")

# COMMAND ----------

display(
    df_rejected_sales
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # INGESTA SILVER

# COMMAND ----------

# MAGIC %md
# MAGIC CUSTOMER

# COMMAND ----------

(
    df_customers_silver.write
    .mode("overwrite")
    .insertInto(
        f"{SILVER_SCHEMA}.customers"
    )
)

# COMMAND ----------

silver_customers = spark.table(
    f"{SILVER_SCHEMA}.customers"
)

print(
    "Registros Silver Customers:",
    silver_customers.count()
)

display(silver_customers.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCS

# COMMAND ----------

(
    df_products_silver.write
    .mode("overwrite")
    .insertInto(
        f"{SILVER_SCHEMA}.products"
    )
)

# COMMAND ----------

print(
    "Silver Products:",
    spark.table(
        f"{SILVER_SCHEMA}.products"
    ).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC SALES

# COMMAND ----------

(
    df_sales_silver.write
    .mode("overwrite")
    .insertInto(f"{SILVER_SCHEMA}.sales")
)

# COMMAND ----------

(
    df_rejected_sales.write
    .mode("overwrite")
    .insertInto(f"{SILVER_SCHEMA}.rejected_sales")
)

# COMMAND ----------

silver_tables = [
    "customers",
    "products",
    "sales",
    "rejected_sales"
]

print("===== SILVER FINAL =====")

for table in silver_tables:
    count = spark.table(
        f"{SILVER_SCHEMA}.{table}"
    ).count()

    print(f"{table}: {count:,} registros")

# COMMAND ----------

display(
    spark.table(f"{SILVER_SCHEMA}.rejected_sales")
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)