# Databricks notebook source
# CONFIGURACION

CATALOG = "andinastore_dev"

# False = simulacion
# True  = ejecuta realmente los DROP
EXECUTE_ROLLBACK = False

print(f"Catalogo objetivo: {CATALOG}")
print(f"Rollback activo:   {EXECUTE_ROLLBACK}")

# COMMAND ----------

# TABLAS GOLD

GOLD_TABLES = [
    "fact_sales",
    "dim_date",
    "dim_product",
    "dim_customer"
]

print("Tablas Gold:")
for table in GOLD_TABLES:
    print(f"- {CATALOG}.gold.{table}")

# COMMAND ----------

# TABLAS SILVER
SILVER_TABLES = [
    "rejected_sales",
    "sales",
    "products",
    "customers"
]

print("Tablas Silver:")
for table in SILVER_TABLES:
    print(f"- {CATALOG}.silver.{table}")

# COMMAND ----------

# TABLAS BRONZE

BRONZE_TABLES = [
    "sales",
    "products",
    "customers"
]

print("Tablas Bronze:")
for table in BRONZE_TABLES:
    print(f"- {CATALOG}.bronze.{table}")

# COMMAND ----------

# PLAN DE REVERSION - TABLAS
TABLES = {
    "gold": GOLD_TABLES,
    "silver": SILVER_TABLES,
    "bronze": BRONZE_TABLES
}

drop_table_statements = []

for schema, tables in TABLES.items():

    for table in tables:

        statement = (
            f"DROP TABLE IF EXISTS "
            f"{CATALOG}.{schema}.{table}"
        )

        drop_table_statements.append(statement)


print("DROP TABLE planificados:\n")

for statement in drop_table_statements:
    print(statement)

# COMMAND ----------

# PLAN DE REVERSION - SCHEMAS

SCHEMAS = [
    "gold",
    "silver",
    "bronze"
]

drop_schema_statements = []

for schema in SCHEMAS:

    statement = (
        f"DROP SCHEMA IF EXISTS "
        f"{CATALOG}.{schema}"
    )

    drop_schema_statements.append(statement)


print("DROP SCHEMA planificados:\n")

for statement in drop_schema_statements:
    print(statement)


# COMMAND ----------

# PLAN DE REVERSION - CATALOGO

drop_catalog_statement = (
    f"DROP CATALOG IF EXISTS {CATALOG}"
)

print(drop_catalog_statement)

# COMMAND ----------

# PLAN COMPLETO DE REVERSION


rollback_statements = (
    drop_table_statements
    + drop_schema_statements
    + [drop_catalog_statement]
)

print("=" * 70)
print("PLAN COMPLETO DE ROLLBACK")
print("=" * 70)

for i, statement in enumerate(
    rollback_statements,
    start=1
):
    print(f"{i:02d}. {statement}")

print(
    f"\nTotal operaciones planificadas: "
    f"{len(rollback_statements)}"
)

# COMMAND ----------


# EJECUCION CONTROLADA

if EXECUTE_ROLLBACK:

    print("=" * 70)
    print("ADVERTENCIA - EJECUTANDO ROLLBACK DEV")
    print("=" * 70)

    for i, statement in enumerate(
        rollback_statements,
        start=1
    ):

        spark.sql(statement)

        print(
            f"[OK] {i}/{len(rollback_statements)} "
            f"{statement}"
        )

    print("\nROLLBACK DEV COMPLETADO.")

else:

    print("=" * 70)
    print("MODO SIMULACION")
    print("=" * 70)

    print("No se elimino ningun objeto.")
    print("EXECUTE_ROLLBACK=False.")
    print("El ambiente DEV permanece intacto.")