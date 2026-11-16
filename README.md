#  AndinaStore Analytics

## Plataforma de Ingeniería de Datos en Azure y Databricks

**AndinaStore Analytics** es un proyecto académico de Ingeniería de Datos que implementa una plataforma analítica de ventas para una empresa de e-commerce utilizando servicios de Microsoft Azure, Azure Databricks, Delta Lake, Unity Catalog, GitHub Actions y Power BI Desktop.

El proyecto cubre el ciclo completo del dato:

**ingesta → procesamiento → calidad → modelado → gobierno → automatización → producción → analítica**

La solución implementa una arquitectura Medallion con ambientes separados de **DEV y PROD**, control de acceso mediante **Unity Catalog**, autenticación contra ADLS Gen2 mediante **Managed Identity**, automatización CI/CD con **GitHub Actions** y consumo del modelo analítico desde **Power BI Desktop** mediante un **Databricks SQL Warehouse**.

---

# 1. Caso de negocio

AndinaStore es una empresa de e-commerce que requiere disponer de información confiable para analizar el comportamiento de sus ventas.

La plataforma permite analizar información relacionada con:

- ventas netas;
- transacciones;
- unidades vendidas;
- productos;
- categorías;
- clientes;
- ciudades;
- comportamiento temporal de las ventas;
- canales;
- métodos de pago.

El objetivo técnico consiste en transformar archivos transaccionales en un modelo analítico gobernado y disponible para herramientas de Business Intelligence.

---

# 2. Arquitectura
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/3149badc-5e5c-403a-9435-5a029f0b46eb" />




La seguridad, gobierno y automatización se implementan transversalmente mediante:

- Managed Identity
- Access Connector
- Unity Catalog
- Storage Credential
- External Location
- grupos y permisos
- Databricks Workflows
- Git
- GitHub
- GitHub Actions
- Databricks REST API

## Componentes principales

| Componente | Función |
|---|---|
| Azure Data Lake Storage Gen2 | Almacenamiento de archivos RAW |
| Azure Databricks | Procesamiento de datos |
| PySpark | Implementación principal del ETL |
| Delta Lake | Persistencia de las capas Medallion |
| Unity Catalog | Gobierno y seguridad |
| Managed Identity | Acceso seguro a almacenamiento |
| Databricks Workflows | Orquestación del ETL |
| Serverless Compute | Ejecución del Workflow PROD |
| Databricks SQL Warehouse | Endpoint SQL para consumo analítico |
| Power BI Desktop | Visualización y análisis |
| GitHub | Versionamiento |
| GitHub Actions | CI/CD DEV → PROD |
| Databricks REST API | Despliegue y ejecución automatizada |

---

# 4. Datos de origen

El proyecto utiliza tres datasets de e-commerce:

| Dataset | Registros |
|---|---:|
| `customers.csv` | 1.000 |
| `products.csv` | 100 |
| `sales.csv` | 50.000 |

Los archivos se almacenan en:

```text
ADLS Gen2
└── Container: raw
    ├── customers.csv
    ├── products.csv
    └── sales.csv
```

Storage Account:

```text
standinastoredata
```

El container RAW funciona como fuente externa del pipeline.

No se utiliza DBFS como almacenamiento RAW.

---

# 5. Arquitectura Medallion

La transformación de datos se implementa mediante las capas:

```text
RAW
 ↓
BRONZE
 ↓
SILVER
 ↓
GOLD
```

Las capas analíticas son almacenadas utilizando **Delta Lake** y gobernadas mediante **Unity Catalog**.

---

# 6. Bronze

Bronze representa la primera capa persistente del Lakehouse.

Su responsabilidad es ingerir los archivos almacenados en el container RAW conservando los datos con mínima transformación.

Tablas:

```text
bronze.customers
bronze.products
bronze.sales
```

Conteos:

| Tabla | Registros |
|---|---:|
| customers | 1.000 |
| products | 100 |
| sales | 50.000 |

Se agregan los siguientes metadatos de trazabilidad:

```text
ingestion_timestamp
source_file
```

Para identificar el archivo origen se utiliza:

```text
metadata.file_path
```

Esto permite conservar trazabilidad sobre la procedencia de cada registro.

---

# 7. Calidad de datos

Los datasets contienen intencionalmente diferentes problemas de calidad para comprobar el funcionamiento del proceso de limpieza.

## Customers

Se incluyen casos como:

- `customer_id` duplicados;
- emails vacíos;
- departamentos vacíos;
- inconsistencias en nombres de ciudades.

## Products

Se incluyen:

- registros duplicados;
- marcas vacías;
- categorías inconsistentes;
- precios negativos;
- costos iguales a cero.

## Sales

Se incluyen:

- ventas duplicadas;
- cantidades iguales a cero;
- clientes inexistentes;
- productos inexistentes;
- descuentos vacíos;
- fechas inválidas;
- descuentos fuera del rango permitido.

Estas condiciones son procesadas durante la transformación hacia Silver.

---

# 8. Silver

Silver contiene información limpia, normalizada y validada.

Tablas:

```text
silver.customers
silver.products
silver.sales
silver.rejected_sales
```

## Resultados

### Customers

```text
Bronze: 1.000
Silver:   990
```

### Products

```text
Bronze: 100
Silver:   93
```

### Sales

```text
Bronze:                  50.000
Después de duplicados:   49.950
Ventas válidas:          48.776
Ventas rechazadas:        1.174
```

Reconciliación:

```text
48.776 + 1.174 = 49.950
```

El proceso utiliza validaciones tolerantes a errores mediante funciones como:

```text
try_to_date
try_cast
```

Además se generan indicadores internos para identificar diferentes tipos de errores.

Entre las razones de rechazo se encuentran:

```text
INVALID_DATE
INVALID_QUANTITY
INVALID_DISCOUNT
CUSTOMER_NOT_FOUND
PRODUCT_NOT_FOUND
```

Los registros rechazados se conservan en:

```text
silver.rejected_sales
```

Esto permite mantener trazabilidad sobre los datos que no superaron las reglas de calidad.

---

# 9. Gold

Gold representa la capa analítica final del Lakehouse.

En esta capa se implementa un **modelo dimensional tipo estrella**.

```text
                 dim_customer
                       │
                       │
dim_date ──────── fact_sales ──────── dim_product
```

Tablas:

```text
gold.dim_customer
gold.dim_product
gold.dim_date
gold.fact_sales
```

Conteos:

| Tabla | Registros |
|---|---:|
| dim_customer | 990 |
| dim_product | 93 |
| dim_date | 680 |
| fact_sales | 48.776 |

---

# 10. Validación del modelo estrella

Se realizaron controles sobre `fact_sales` para comprobar la integridad del modelo.

Resultados:

```text
sale_id únicos            = 48.776
claves nulas              = 0
clientes huérfanos        = 0
productos huérfanos       = 0
fechas huérfanas          = 0
```

La tabla de hechos incluye métricas analíticas como:

```text
gross_amount
discount_amount
net_sales
total_cost
profit
```

Las dimensiones permiten analizar estas métricas desde diferentes perspectivas de negocio.

---

# 11. Unity Catalog

El gobierno de la plataforma se implementa mediante Unity Catalog.

Metastore:

```text
metastore-andinastore
```

Se mantienen catálogos independientes para cada ambiente.

## DEV

```text
andinastore_dev
├── bronze
├── silver
└── gold
```

## PROD

```text
andinastore_prod
├── bronze
├── silver
└── gold
```

Los dos workspaces utilizan el mismo metastore manteniendo separación lógica entre ambientes.

---

# 12. Integración segura con ADLS Gen2

El acceso de Databricks al almacenamiento se realiza sin utilizar credenciales estáticas.

Componentes:

```text
Access Connector
ac-andinastore-databricks

Managed Identity
mi-andinastore

Storage Credential
sc-andinastore-adls

External Location
ext-andinastore-raw
```

Ruta RAW:

```text
abfss://raw@standinastoredata.dfs.core.windows.net/
```

El diseño evita utilizar:

- Storage Keys;
- SAS;
- passwords;
- client secrets para acceder al Storage.

El acceso se realiza mediante **Managed Identity y RBAC**.

---

# 13. Seguridad

Se implementaron cuatro grupos funcionales:

```text
andinastore_admins
andinastore_developers
andinastore_etl
andinastore_analysts
```

## DEV

| Grupo | Acceso |
|---|---|
| Admins | Administración + lectura/escritura |
| Developers | R/W/Create Bronze, Silver y Gold |
| ETL | R/W/Create Bronze, Silver y Gold |
| Analysts | SELECT Gold |

Developers y ETL disponen además del acceso requerido para leer RAW.

## PROD

| Grupo | Acceso |
|---|---|
| Admins | Administración + lectura/escritura |
| ETL | R/W/Create Bronze, Silver y Gold |
| Developers | SELECT Gold |
| Analysts | SELECT Gold |

Durante la integración con Power BI se validaron además los permisos necesarios para navegar la jerarquía de Unity Catalog:

```text
USE CATALOG
USE SCHEMA
SELECT
```

Esto permite que los consumidores autorizados accedan exclusivamente a la información analítica correspondiente.

---

# 14. Ambientes DEV y PROD

La solución utiliza dos workspaces independientes.

## DEV

```text
dbw-andinastore-dev
```

## PROD

```text
dbw-andinastore-prod
```

Los procesos fueron parametrizados mediante:

```python
dbutils.widgets.text(
    "ENVIRONMENT",
    "dev",
    "Environment: dev | prod"
)
```

Los valores permitidos son:

```text
dev
prod
```

El catálogo se determina dinámicamente:

```text
andinastore_{ENVIRONMENT}
```

Esto permite reutilizar los mismos notebooks entre DEV y PROD evitando mantener versiones independientes del código ETL.

---

# 15. Proceso ETL

Los notebooks principales son:

```text
PreAmb/
└── 01_preparacion_ambiente

proceso/
├── 01_extract
├── 02_transform
├── 03_load
└── 04_grants
```

El proceso sigue la secuencia:

```text
01_preparacion_ambiente
        ↓
01_extract
        ↓
02_transform
        ↓
03_load
        ↓
04_grants
```

El ETL utiliza principalmente **PySpark**, mientras que las operaciones administrativas de Unity Catalog utilizan SQL cuando corresponde.

---

# 16. Orquestación

En PROD se implementó el Workflow:

```text
ETL ANDINASTORE
```

Flujo:

```text
01_preparacion_ambiente
        ↓
01_extract
        ↓
02_transform
        ↓
03_load
        ↓
04_grants
```

Todas las tareas reciben:

```text
ENVIRONMENT = prod
```

Para la ejecución productiva se utiliza **Serverless Compute**.

El Workflow fue probado de extremo a extremo y todas sus Tasks finalizaron correctamente.

---

# 17. Validación PROD

Después de ejecutar el pipeline productivo se obtuvieron los siguientes conteos:

| Capa | Tabla | Registros |
|---|---|---:|
| Bronze | customers | 1.000 |
| Bronze | products | 100 |
| Bronze | sales | 50.000 |
| Silver | customers | 990 |
| Silver | products | 93 |
| Silver | sales | 48.776 |
| Silver | rejected_sales | 1.174 |
| Gold | dim_customer | 990 |
| Gold | dim_product | 93 |
| Gold | dim_date | 680 |
| Gold | fact_sales | 48.776 |

Los resultados coinciden con los obtenidos previamente en DEV.

---

# 18. Git y control de versiones

Repositorio:

```text
AndinaStore-Analytics
```

Ramas principales:

```text
main
andinastore-dev
```

El desarrollo no se realiza directamente sobre `main`.

Flujo:

```text
Databricks DEV
      ↓
Git Folder
      ↓
andinastore-dev
      ↓
Commit & Push
      ↓
GitHub
      ↓
Pull Request
      ↓
main
```

Databricks DEV utiliza un Git Folder conectado con la rama:

```text
andinastore-dev
```

---

# 19. CI/CD

Se implementó CI/CD mediante:

```text
GitHub Actions
+
Databricks REST API
+
curl
+
jq
```

Workflow:

```text
.github/workflows/deploy-prod.yml
```

El pipeline se activa cuando los cambios llegan a:

```text
main
```

También permite ejecución manual mediante:

```text
workflow_dispatch
```

## Flujo de despliegue

```text
Databricks DEV
      ↓
Git / andinastore-dev
      ↓
Pull Request
      ↓
main
      ↓
GitHub Actions
      ↓
Export notebooks DEV
      ↓
Deploy PROD
      ↓
Run ETL ANDINASTORE
      ↓
Monitor Tasks
      ↓
SUCCESS
```

GitHub Actions utiliza las APIs REST de Databricks para:

1. obtener los notebooks desde DEV;
2. preparar las carpetas correspondientes en PROD;
3. importar o sobrescribir los notebooks;
4. localizar el Workflow `ETL ANDINASTORE`;
5. ejecutar el Job;
6. obtener el `RUN_ID`;
7. monitorear las Tasks;
8. comprobar el resultado final;
9. eliminar los archivos temporales.

La ejecución real fue validada satisfactoriamente.

Resultado:

```text
01_preparacion_ambiente   TERMINATED / SUCCESS
01_extract                TERMINATED / SUCCESS
02_transform              TERMINATED / SUCCESS
03_load                   TERMINATED / SUCCESS
04_grants                 TERMINATED / SUCCESS
```

Mensaje final:

```text
WORKFLOW COMPLETADO CORRECTAMENTE
```

---

# 20. Reversión

El proyecto incluye mecanismos controlados de rollback.

```text
reversion/
├── 01_rollback_dev
└── 02_rollback_prod
```

El orden de reversión es:

```text
Gold
 ↓
Silver
 ↓
Bronze
 ↓
Schemas
 ↓
Catalog
```

Los procedimientos no eliminan:

- archivos RAW;
- Storage Account;
- Managed Identity;
- Access Connector;
- Metastore;
- Storage Credential;
- External Location;
- Workspaces.

PROD incorpora una protección adicional:

```text
EXECUTE_ROLLBACK = False
```

Además requiere confirmación explícita antes de una eliminación.

El rollback de PROD fue validado exclusivamente en **modo simulación**.

No se eliminaron objetos productivos.

---

# 21. Databricks SQL Warehouse

El modelo analítico Gold se expone a Power BI utilizando un **Databricks SQL Warehouse**.

El SQL Warehouse funciona como **endpoint de consulta**, no como una nueva capa de almacenamiento ni como una capa de serving adicional.

El flujo es:

```text
Gold / Delta Lake
       ↓
Databricks SQL Warehouse
       ↓
Power BI Desktop
```

Power BI consume directamente:

```text
andinastore_prod.gold.dim_customer
andinastore_prod.gold.dim_product
andinastore_prod.gold.dim_date
andinastore_prod.gold.fact_sales
```

Por tanto, no existe duplicación adicional del modelo Gold en otra plataforma de base de datos.

---

# 22. Power BI Desktop

Power BI Desktop funciona como capa de visualización y análisis de la plataforma.

El modelo conserva las relaciones:

```text
dim_customer  1 ──── N  fact_sales

dim_product   1 ──── N  fact_sales

dim_date      1 ──── N  fact_sales
```

Entre las métricas utilizadas se encuentran:

```text
Ventas Netas
Transacciones
Unidades Vendidas
```

El dashboard permite analizar:

- ventas netas;
- transacciones;
- unidades vendidas;
- ventas por categoría;
- Top 10 productos;
- comportamiento temporal;
- clientes y ciudades.

## Páginas

### Resumen Ejecutivo

Presenta los principales indicadores comerciales y una visión general del comportamiento del negocio.

### Análisis de Productos

Permite profundizar en:

- productos con mayores ventas;
- ventas por categoría;
- unidades vendidas;
- evolución de ventas.

El archivo `.pbix` y las evidencias correspondientes se almacenan en:

```text
dashboard/
```

Para la entrega académica se utiliza **Power BI Desktop**, sin dependencia de Power BI Service.

---

# 23. Tecnologías utilizadas

| Área | Tecnología |
|---|---|
| Cloud | Microsoft Azure |
| Data Lake | ADLS Gen2 |
| Procesamiento | Azure Databricks |
| Lenguaje ETL | PySpark |
| Formato Lakehouse | Delta Lake |
| Gobierno | Unity Catalog |
| Identidad | Managed Identity |
| Seguridad Azure | RBAC |
| Orquestación | Databricks Workflows |
| Compute PROD | Serverless |
| Consumo SQL | Databricks SQL Warehouse |
| BI | Power BI Desktop |
| Versionamiento | Git / GitHub |
| CI/CD | GitHub Actions |
| Automatización | Databricks REST API |

---

# 24. Estructura del repositorio

```text
AndinaStore-Analytics/
│
├── .github/
│   └── workflows/
│       └── deploy-prod.yml
│
├── certificaciones/
├── dashboard/
├── datasets/
├── evidencias/
├── PreAmb/
├── proceso/
├── reversion/
├── seguridad/
│
├── databricks.yml
└── README.md
```

## Carpetas principales

### `datasets/`

Contiene los tres datasets utilizados:

```text
customers.csv
products.csv
sales.csv
```

### `PreAmb/`

Contiene la preparación inicial parametrizada del ambiente.

### `proceso/`

Contiene los notebooks principales del ETL.

### `seguridad/`

Contiene los artefactos relacionados con seguridad y permisos.

### `reversion/`

Contiene los mecanismos de rollback para DEV y PROD.

### `dashboard/`

Contiene el archivo de Power BI y las evidencias del dashboard.

### `evidencias/`

Contiene capturas y evidencias técnicas de la implementación.

### `.github/workflows/`

Contiene el pipeline CI/CD.

---

# 25. Principales resultados

El proyecto logró implementar y validar:

-  almacenamiento RAW en ADLS Gen2;
-  acceso seguro mediante Managed Identity;
-  Access Connector;
-  RBAC;
-  Unity Catalog;
-  Storage Credential;
-  External Location;
-  arquitectura Medallion;
-  procesamiento Bronze;
-  limpieza y calidad Silver;
-  gestión de registros rechazados;
-  modelo estrella Gold;
-  validación de integridad referencial;
-  separación DEV / PROD;
-  seguridad mediante grupos;
-  ETL parametrizado;
-  Databricks Workflow;
-  Serverless Compute en PROD;
-  rollback controlado;
-  Git y ramas;
-  Pull Request;
-  GitHub Actions;
-  CI/CD DEV → PROD;
-  ejecución automática del Workflow;
-  Databricks SQL Warehouse;
-  integración Gold → Power BI Desktop;
-  dashboard analítico.

---

# 26. Evidencias

El repositorio incluye evidencias de los principales componentes implementados, incluyendo:

- Resource Group;
- ADLS Gen2;
- container RAW;
- Managed Identity;
- Access Connector;
- IAM/RBAC;
- Unity Catalog Metastore;
- workspaces DEV y PROD;
- catálogos y schemas;
- Storage Credential;
- External Location;
- Bronze;
- Silver;
- `rejected_sales`;
- Gold;
- `fact_sales`;
- modelo estrella;
- grupos y permisos;
- rollback;
- Workflow PROD;
- Serverless Compute;
- integración Git;
- ramas;
- Pull Request;
- GitHub Actions;
- despliegue PROD;
- Tasks ejecutadas correctamente;
- Databricks SQL Warehouse;
- Power BI Desktop;
- modelo Power BI;
- dashboard final.

Una de las principales evidencias del CI/CD corresponde a la ejecución exitosa con:

```text
WORKFLOW COMPLETADO CORRECTAMENTE
```

---

# 27. Conclusión

**AndinaStore Analytics** demuestra la implementación de una plataforma moderna de Ingeniería de Datos utilizando tecnologías del ecosistema Microsoft Azure y Databricks.

La solución integra almacenamiento, procesamiento distribuido, calidad de datos, arquitectura Medallion, Delta Lake, gobierno mediante Unity Catalog, identidad administrada, seguridad basada en roles, separación de ambientes, orquestación, control de versiones, CI/CD y Business Intelligence.

El resultado final establece un flujo completo:

```text
CSV
 ↓
ADLS Gen2 / RAW
 ↓
Azure Databricks
 ↓
Bronze
 ↓
Silver
 ↓
Gold / Modelo Estrella
 ↓
Databricks SQL Warehouse
 ↓
Power BI Desktop
 ↓
Dashboard Analítico
```

De esta manera, los datos transaccionales se convierten en información gobernada, confiable y disponible para el análisis y la toma de decisiones.

---

## Autor

**Juan David Capera Tovar**

Proyecto Final de Ingeniería de Datos  
AndinaStore Analytics
