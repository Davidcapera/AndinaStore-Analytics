# Databricks notebook source

# ROLLBACK CONTROLADO - PRODUCCION

ENVIRONMENT = "prod"

EXECUTE_ROLLBACK = False

CONFIRMATION = ""

REQUIRED_CONFIRMATION = "DELETE_ANDINASTORE_PROD"

CATALOG = f"andinastore_{ENVIRONMENT}"

BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# COMMAND ----------

# VALIDACIONES DE SEGURIDAD

ENVIRONMENT = ENVIRONMENT.strip().lower()

if ENVIRONMENT != "prod":
    raise ValueError(
        "Este notebook esta diseñado exclusivamente para PROD."
    )

if CATALOG != "andinastore_prod":
    raise ValueError(
        f"Catalogo PROD inesperado: {CATALOG}"
    )

if EXECUTE_ROLLBACK and CONFIRMATION != REQUIRED_CONFIRMATION:
    raise ValueError(
        "ROLLBACK PROD BLOQUEADO: "
        "la confirmacion explicita no coincide."
    )

print("==============================================")
print("ROLLBACK CONTROLADO - ANDINASTORE PROD")
print("==============================================")
print(f"Catalogo           : {CATALOG}")
print(f"Ejecutar rollback  : {EXECUTE_ROLLBACK}")

if EXECUTE_ROLLBACK:
    print("MODO DESTRUCCION AUTORIZADO")
else:
    print("MODO SIMULACION")

# COMMAND ----------

# GOLD

gold_tables = [
    f"{GOLD_SCHEMA}.fact_sales",
    f"{GOLD_SCHEMA}.dim_date",
    f"{GOLD_SCHEMA}.dim_product",
    f"{GOLD_SCHEMA}.dim_customer"
]

# COMMAND ----------

# SILVER

silver_tables = [
    f"{SILVER_SCHEMA}.rejected_sales",
    f"{SILVER_SCHEMA}.sales",
    f"{SILVER_SCHEMA}.products",
    f"{SILVER_SCHEMA}.customers"
]

# COMMAND ----------

# BRONZE

bronze_tables = [
    f"{BRONZE_SCHEMA}.sales",
    f"{BRONZE_SCHEMA}.products",
    f"{BRONZE_SCHEMA}.customers"
]

# COMMAND ----------

# MAGIC %md
# MAGIC # Funcion protegida

# COMMAND ----------

# FUNCION DE ROLLBACK

def execute_or_simulate(sql_command):

    if EXECUTE_ROLLBACK:
        print(f"EJECUTANDO: {sql_command}")
        spark.sql(sql_command)
    else:
        print(f"[SIMULACION] {sql_command}")

# COMMAND ----------

# ROLLBACK GOLD

for table in gold_tables:
    execute_or_simulate(
        f"DROP TABLE IF EXISTS {table}"
    )

# COMMAND ----------

# ROLLBACK SILVER

for table in silver_tables:
    execute_or_simulate(
        f"DROP TABLE IF EXISTS {table}"
    )

# COMMAND ----------

# ROLLBACK BRONZE

for table in bronze_tables:
    execute_or_simulate(
        f"DROP TABLE IF EXISTS {table}"
    )

# COMMAND ----------

