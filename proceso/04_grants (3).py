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
EXTERNAL_LOCATION = "ext-andinastore-raw"

GROUP_ADMIN = "andinastore_admins"
GROUP_DEVELOPER = "andinastore_developers"
GROUP_ETL = "andinastore_etl"
GROUP_ANALYST = "andinastore_analysts"
SCHEMAS = ["bronze", "silver", "gold"]

print("=" * 70)
print("ANDINASTORE - CONFIGURACION DE SEGURIDAD")
print("=" * 70)
print(f"Ambiente: {ENVIRONMENT.upper()}")
print(f"Catalogo: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC # PERMISOS A CATALOG

# COMMAND ----------

GROUPS = [GROUP_ADMIN, GROUP_DEVELOPER, GROUP_ETL, GROUP_ANALYST]

print("=" * 70)
print("ANDINASTORE - CONFIGURACION USE CATALOG")
print("=" * 70)

for group in GROUPS:
    spark.sql(f"""
        GRANT USE CATALOG
        ON CATALOG {CATALOG}
        TO `{group}`
    """)
    print(f"[OK] USE CATALOG -> {group}")

# COMMAND ----------

print("=" * 70)
print("VALIDACION DE GRANTS - CATALOGO")
print("=" * 70)
spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # PERMISOS A SCHEMA

# COMMAND ----------

if ENVIRONMENT == "dev":
    FULL_SCHEMA_GROUPS = [GROUP_ADMIN, GROUP_DEVELOPER, GROUP_ETL]
else:
    FULL_SCHEMA_GROUPS = [GROUP_ADMIN, GROUP_ETL]

for group in FULL_SCHEMA_GROUPS:
    for schema in SCHEMAS:
        spark.sql(f"""
            GRANT USE SCHEMA
            ON SCHEMA {CATALOG}.{schema}
            TO `{group}`
        """)
        print(f"[OK] USE SCHEMA {CATALOG}.{schema} -> {group}")

# Developer PROD: solo Gold
if ENVIRONMENT == "prod":
    spark.sql(f"""
        GRANT USE SCHEMA
        ON SCHEMA {GOLD_SCHEMA}
        TO `{GROUP_DEVELOPER}`
    """)

# Analyst DEV/PROD: solo Gold
spark.sql(f"""
    GRANT USE SCHEMA
    ON SCHEMA {GOLD_SCHEMA}
    TO `{GROUP_ANALYST}`
""")

# COMMAND ----------

print("=" * 70)
print("VALIDACION DE GRANTS - SCHEMAS")
print("=" * 70)
for schema in SCHEMAS:
    print(f"\nSCHEMA: {CATALOG}.{schema}")
    spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{schema}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # PERMISOS SELECT

# COMMAND ----------

if ENVIRONMENT == "dev":
    SELECT_ALL_GROUPS = [GROUP_ADMIN, GROUP_DEVELOPER, GROUP_ETL]
else:
    SELECT_ALL_GROUPS = [GROUP_ADMIN, GROUP_ETL]

for group in SELECT_ALL_GROUPS:
    for schema in SCHEMAS:
        spark.sql(f"""
            GRANT SELECT
            ON SCHEMA {CATALOG}.{schema}
            TO `{group}`
        """)
        print(f"[OK] SELECT {CATALOG}.{schema} -> {group}")

# Developer PROD: solo lectura Gold
if ENVIRONMENT == "prod":
    spark.sql(f"""
        GRANT SELECT
        ON SCHEMA {GOLD_SCHEMA}
        TO `{GROUP_DEVELOPER}`
    """)

# Analyst DEV/PROD: solo lectura Gold
spark.sql(f"""
    GRANT SELECT
    ON SCHEMA {GOLD_SCHEMA}
    TO `{GROUP_ANALYST}`
""")

# COMMAND ----------

print("=" * 70)
print("VALIDACION DE GRANTS SELECT")
print("=" * 70)
for schema in SCHEMAS:
    print(f"\nSCHEMA: {CATALOG}.{schema}")
    spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{schema}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # PERMISOS MODIFY

# COMMAND ----------

if ENVIRONMENT == "dev":
    MODIFY_GROUPS = [GROUP_ADMIN, GROUP_DEVELOPER, GROUP_ETL]
else:
    MODIFY_GROUPS = [GROUP_ADMIN, GROUP_ETL]

for group in MODIFY_GROUPS:
    for schema in SCHEMAS:
        spark.sql(f"""
            GRANT MODIFY
            ON SCHEMA {CATALOG}.{schema}
            TO `{group}`
        """)
        print(f"[OK] MODIFY {CATALOG}.{schema} -> {group}")

# COMMAND ----------

print("=" * 70)
print("VALIDACION DE GRANTS MODIFY")
print("=" * 70)
for schema in SCHEMAS:
    print(f"\nSCHEMA: {CATALOG}.{schema}")
    spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{schema}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # PERMISOS CREATE TABLE

# COMMAND ----------

if ENVIRONMENT == "dev":
    CREATE_TABLE_GROUPS = [GROUP_ADMIN, GROUP_DEVELOPER, GROUP_ETL]
else:
    CREATE_TABLE_GROUPS = [GROUP_ADMIN, GROUP_ETL]

for group in CREATE_TABLE_GROUPS:
    for schema in SCHEMAS:
        spark.sql(f"""
            GRANT CREATE TABLE
            ON SCHEMA {CATALOG}.{schema}
            TO `{group}`
        """)
        print(f"[OK] CREATE TABLE {CATALOG}.{schema} -> {group}")

# COMMAND ----------

print("=" * 70)
print("VALIDACION DE GRANTS CREATE TABLE")
print("=" * 70)
for schema in SCHEMAS:
    print(f"\nSCHEMA: {CATALOG}.{schema}")
    spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{schema}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # READ FILES RAW

# COMMAND ----------

if ENVIRONMENT == "dev":
    RAW_READ_GROUPS = [GROUP_DEVELOPER, GROUP_ETL]
else:
    RAW_READ_GROUPS = [GROUP_ETL]

for group in RAW_READ_GROUPS:
    spark.sql(f"""
        GRANT READ FILES
        ON EXTERNAL LOCATION `{EXTERNAL_LOCATION}`
        TO `{group}`
    """)
    print(f"[OK] READ FILES {EXTERNAL_LOCATION} -> {group}")

# COMMAND ----------

print("=" * 70)
print("VALIDACION READ FILES RAW")
print("=" * 70)
spark.sql(f"""
    SHOW GRANTS ON EXTERNAL LOCATION `{EXTERNAL_LOCATION}`
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE SCHEMA

# COMMAND ----------

spark.sql(f"""
    GRANT CREATE SCHEMA
    ON CATALOG {CATALOG}
    TO `{GROUP_ADMIN}`
""")
print(f"[OK] CREATE SCHEMA {CATALOG} -> {GROUP_ADMIN}")

# COMMAND ----------

print("=" * 70)
print("VALIDACION CREATE SCHEMA")
print("=" * 70)
spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDACION FINAL

# COMMAND ----------

print("=" * 80)
print(f"ANDINASTORE - VALIDACION FINAL SEGURIDAD {ENVIRONMENT.upper()}")
print("=" * 80)

print("\n[1] IDENTIDAD ACTUAL")
spark.sql("SELECT current_user() AS current_user").show(truncate=False)

print("\n[2] CATALOGO")
spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG}").show(truncate=False)

print("\n[3] SCHEMAS")
for schema in SCHEMAS:
    print(f"\n--- {CATALOG}.{schema} ---")
    spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{schema}").show(truncate=False)

print("\n[4] EXTERNAL LOCATION RAW")
spark.sql(f"""
    SHOW GRANTS ON EXTERNAL LOCATION `{EXTERNAL_LOCATION}`
""").show(truncate=False)

print("\n[5] OWNERSHIP CATALOGO")
spark.sql(f"DESCRIBE CATALOG EXTENDED {CATALOG}").show(truncate=False)

print("\n[6] OWNERSHIP EXTERNAL LOCATION RAW")
spark.sql(f"""
    DESCRIBE EXTERNAL LOCATION `{EXTERNAL_LOCATION}`
""").show(truncate=False)

print("\n" + "=" * 80)
print(f"VALIDACION {ENVIRONMENT.upper()} FINALIZADA")
print("=" * 80)