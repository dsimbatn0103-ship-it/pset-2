# PSet 2 — NY Taxi ELT Pipeline

## Objetivo del proyecto

Construir una solución ELT end-to-end para ingerir, almacenar, transformar y modelar datos
históricos de taxis amarillos de Nueva York (2023–2024), desplegada completamente con
Docker Compose. El sistema separa los datos en dos capas dentro de una única base de datos
PostgreSQL: una capa `raw` con los datos tal como llegan de la fuente, y una capa `clean`
con un modelo dimensional analítico listo para consumo.

---

## Arquitectura

```
Fuente NY Taxi (.parquet)
         │
         ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  Docker Compose — red pset2-net (bridge)                     │
 │                                                              │
 │  ┌─────────────────────────────────────────────────────┐     │
 │  │  Mage AI :6789                                      │     │
 │  │                                                     │     │
 │  │  Pipeline 1 — raw_ingestion_pipeline                │     │
 │  │    Data Loader → Transformer → Data Exporter        │──►  │
 │  │                                                     │     │
 │  │  Pipeline 2 — clean_transformation_pipeline         │     │
 │  │    Data Loader → Transformer → Data Exporter        │──►  │
 │  └─────────────────────────────────────────────────────┘     │
 │                                                              │
 │  ┌─────────────────────────────┐  ┌──────────────────────┐  │
 │  │  PostgreSQL :5432           │  │  pgAdmin :9000       │  │
 │  │  DB: warehouse              │  │  (inspección UI)     │  │
 │  │  ├── schema raw             │  └──────────────────────┘  │
 │  │  │   └── viajes_taxi_…      │                            │
 │  │  └── schema clean           │                            │
 │  │      ├── fact_viajes        │                            │
 │  │      ├── dim_fecha          │                            │
 │  │      ├── dim_vendor         │                            │
 │  │      ├── dim_payment_type   │                            │
 │  │      ├── dim_ratecode       │                            │
 │  │      └── dim_location       │                            │
 │  └─────────────────────────────┘                            │
 └──────────────────────────────────────────────────────────────┘
```

Una sola base de datos (`warehouse`) con dos schemas (`raw` y `clean`). No se usan dos
bases de datos porque los schemas de PostgreSQL proporcionan el mismo aislamiento lógico
con menos complejidad operativa.

---

## Estructura del proyecto

```
pset2_ny_taxi/
├── docker-compose.yaml          ← Orquestación de servicios
├── .env                         ← Variables de entorno 
├── README.md
│
├── mage-volume/                 ← Volumen montado en el contenedor de Mage
│   ├── io_config.yaml           ← Configuración de conexiones (usa secrets)
│   ├── data_loaders/
│   │   ├── load_raw_parquet.py  ← Pipeline RAW — Bloque 1
│   │   └── build_dimensions.py  ← Pipeline CLEAN — Bloque 1
│   ├── transformers/
│   │   ├── validate_raw.py      ← Pipeline RAW — Bloque 2
│   │   └── transform_and_load_fact.py  ← Pipeline CLEAN — Bloque 2
│   └── data_exporters/
│       ├── confirm_raw_schema.py   ← Pipeline RAW — Bloque 3
│       └── validate_clean_model.py ← Pipeline CLEAN — Bloque 3
│
├── data-ui-volume/              ← Volumen de pgAdmin (configuración persistente)
│
├── notebooks/
│   ├── validaciones_pset2.ipynb ← Validaciones exploratorias
│   └── alternativa_sql.sql      ← Alternativa SQL pura para pgAdmin
│
├── docs/
│   └── architecture_diagram.png
│
└── screenshots/

```

---

## Pasos para levantar el entorno

### Prerequisitos
- Docker Desktop instalado y corriendo
- Al menos 20 GB de espacio libre en disco
- Al menos 8 GB de RAM disponible

### 1. Clonar o descomprimir el proyecto

```bash
cd pset2_ny_taxi
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env   # si existe, o editar .env directamente
```

### 3. Levantar los servicios

```bash
docker compose up -d
```

Verificar que los tres servicios están corriendo:

```bash
docker compose ps
```

Deberías ver `data-warehouse`, `warehouse-ui` y `orquestador` con status `running`.

### 4. Configurar Secrets en Mage

Abre `http://localhost:6789` → Settings → Secrets → New Secret.
Crea los siguientes cinco secrets:

| Nombre      | Valor          |
|-------------|----------------|
| pg_host     | xxxxxxxxxxxxxx |
| pg_port     | xxxx           |
| pg_db       | xxxxxxxxx      |
| pg_user     | xxxx           |
| pg_password | xxxx           |

Estos secrets son leídos por `io_config.yaml` mediante `mage_secret_var()`.
Ninguna credencial está hardcodeada en el código fuente.

---

## Cómo ejecutar los pipelines

### Pipeline 1 — raw_ingestion_pipeline

1. Mage → Pipelines → `raw_ingestion_pipeline` → Run pipeline
2. Bloques en orden:
   - `load_raw_parquet` — descarga y carga los parquet mes a mes a `raw`
   - `validate_raw` — verifica distribución y calidad básica
   - `confirm_raw_schema` — confirma estructura y guarda log de auditoría

> **Nota de memoria:** el bloque `load_raw_parquet` procesa un mes a la vez (~600 MB
> máximo en RAM) para evitar el crash del kernel. Nunca acumula todos los meses.

**Si ya tienes los datos en `public.viajes_taxi_amarillo`**, ejecuta en pgAdmin:

```sql
CREATE SCHEMA IF NOT EXISTS raw;
ALTER TABLE public.viajes_taxi_amarillo SET SCHEMA raw;
```

Esto es instantáneo — no copia datos físicamente.

### Pipeline 2 — clean_transformation_pipeline

Ejecutar **después** de que el Pipeline 1 haya completado exitosamente.

1. Mage → Pipelines → `clean_transformation_pipeline` → Run pipeline
2. Bloques en orden:
   - `build_dimensions` — crea schema clean, 5 dimensiones y fact_viajes vacía
   - `transform_and_load_fact` — transforma raw → clean usando SQL puro dentro de
     PostgreSQL (psycopg2 con autocommit=True, 0 filas en RAM de Python)
   - `validate_clean_model` — valida integridad referencial y genera métricas

> **Tiempo estimado:** el bloque `transform_and_load_fact` puede tardar 15–45 minutos
> con 79M filas. Es tiempo de procesamiento de PostgreSQL, no de Python.

---

## Triggers configurados

### Trigger RAW
- Nombre: `trigger_raw_carga_inicial`
- Tipo: Schedule — `@once`
- Propósito: ejecuta la carga inicial de datos históricos 2023–2024

### Trigger CLEAN
- Nombre: `trigger_clean_transformacion`
- Tipo: Schedule — `@once` con start_date posterior al trigger RAW
- Propósito: ejecuta la transformación dimensional después de que RAW esté completo
- Dependencia: lógicamente posterior a RAW (el offset de tiempo garantiza el orden)

---

## Cómo acceder a pgAdmin

1. Abre `http://localhost:9000`
2. Credenciales: email y contraseña del `.env`
3. Clic derecho en Servers → Register → Server
4. En la pestaña Connection:
   - Host: `data-warehouse`
   - Port: `xxxx`
   - Database: `xxxx`
   - Username: `xxxx`
   - Password: `xxxx`

---

## Cómo validar resultados en PostgreSQL

### Verificar schema raw

```sql
SELECT
    EXTRACT(YEAR  FROM tpep_pickup_datetime)::int AS anio,
    EXTRACT(MONTH FROM tpep_pickup_datetime)::int AS mes,
    COUNT(*) AS viajes
FROM raw.viajes_taxi_amarillo
GROUP BY 1, 2
ORDER BY 1, 2;
-- Esperado: 24 filas (12 meses × 2 años), ~3M viajes por mes
```

### Verificar modelo dimensional

```sql
-- Conteo de todas las tablas
SELECT 'fact_viajes'       AS tabla, COUNT(*) AS filas FROM clean.fact_viajes       UNION ALL
SELECT 'dim_vendor',                 COUNT(*) FROM clean.dim_vendor                 UNION ALL
SELECT 'dim_payment_type',           COUNT(*) FROM clean.dim_payment_type           UNION ALL
SELECT 'dim_ratecode',               COUNT(*) FROM clean.dim_ratecode               UNION ALL
SELECT 'dim_location',               COUNT(*) FROM clean.dim_location               UNION ALL
SELECT 'dim_fecha',                  COUNT(*) FROM clean.dim_fecha;
```

## Modelo dimensional documentado

### Tabla de hechos: `clean.fact_viajes`

**Granularidad:** una fila = un viaje de taxi completado.

| Columna | Tipo | Descripción |
|---|---|---|
| `viaje_id` | BIGSERIAL PK | Clave surrogate generada automáticamente |
| `fecha_id` | INTEGER FK | → `dim_fecha.fecha_id` |
| `vendor_id` | INTEGER FK | → `dim_vendor.vendor_id` |
| `pickup_location_id` | INTEGER FK | → `dim_location.location_id` |
| `dropoff_location_id` | INTEGER FK | → `dim_location.location_id` |
| `payment_id` | INTEGER FK | → `dim_payment_type.payment_id` |
| `ratecode_id` | INTEGER FK | → `dim_ratecode.ratecode_id` |
| `passenger_count` | INTEGER | Número de pasajeros |
| `trip_distance` | NUMERIC(8,2) | Distancia en millas |
| `fare_amount` | NUMERIC(8,2) | Tarifa base |
| `extra` | NUMERIC(8,2) | Cargos adicionales |
| `mta_tax` | NUMERIC(8,2) | Impuesto MTA |
| `tip_amount` | NUMERIC(8,2) | Propina |
| `tolls_amount` | NUMERIC(8,2) | Peajes |
| `improvement_surcharge` | NUMERIC(8,2) | Recargo de mejora |
| `congestion_surcharge` | NUMERIC(8,2) | Recargo de congestión |
| `airport_fee` | NUMERIC(8,2) | Tarifa aeropuerto |
| `total_amount` | NUMERIC(8,2) | Total cobrado |
| `duracion_minutos` | NUMERIC(8,2) | Duración calculada: dropoff - pickup |
| `pickup_datetime` | TIMESTAMP | Fecha/hora de recogida |
| `dropoff_datetime` | TIMESTAMP | Fecha/hora de entrega |

### Dimensiones

#### `clean.dim_fecha`
| Columna | Tipo | Descripción |
|---|---|---|
| `fecha_id` | SERIAL PK | Clave surrogate |
| `fecha` | DATE UNIQUE | Fecha del viaje |
| `anio` | INTEGER | Año (2023, 2024) |
| `mes` | INTEGER | Mes (1–12) |
| `dia` | INTEGER | Día del mes |
| `dia_semana` | INTEGER | 0=domingo … 6=sábado |
| `es_finde` | BOOLEAN | True si sábado o domingo |

#### `clean.dim_vendor`
| Columna | Tipo | Descripción |
|---|---|---|
| `vendor_id` | INTEGER PK | ID del proveedor (0,1,2) |
| `vendor_nombre` | TEXT | Creative Mobile Technologies / VeriFone Inc |

#### `clean.dim_payment_type`
| Columna | Tipo | Descripción |
|---|---|---|
| `payment_id` | INTEGER PK | ID del método de pago (0–6) |
| `payment_nombre` | TEXT | Tarjeta / Efectivo / Sin cargo / etc. |

#### `clean.dim_ratecode`
| Columna | Tipo | Descripción |
|---|---|---|
| `ratecode_id` | INTEGER PK | ID del tipo de tarifa (0–6) |
| `ratecode_descripcion` | TEXT | Tarifa estándar / JFK / Newark / etc. |

#### `clean.dim_location`
| Columna | Tipo | Descripción |
|---|---|---|
| `location_id` | INTEGER PK | ID de zona TLC (1–265) |
| `borough` | TEXT | Borough de NYC (enriquecible con CSV de TLC) |
| `zone` | TEXT | Nombre de la zona |

> `dim_location` es usada dos veces en `fact_viajes`: como `pickup_location_id`
> y como `dropoff_location_id`. Este patrón de rol múltiple (role-playing dimension)
> evita duplicar la tabla y es estándar en modelamiento dimensional.

### Relaciones

```
dim_fecha       ──┐
dim_vendor      ──┤
dim_payment_type──┼──► fact_viajes (FK)
dim_ratecode    ──┤
dim_location    ──┘ (×2: pickup y dropoff)
```

---

## Decisiones de diseño

**Una base de datos, dos schemas.** PostgreSQL permite separar `raw` y `clean`
con schemas sin necesitar dos instancias. Esto simplifica la red Docker, el
`io_config.yaml` y las queries de validación cruzada entre capas.

**Pipeline RAW no transforma.** Solo aplica tipado mínimo de fechas y normalización
de nombres de columnas (snake_case). La lógica de negocio queda exclusivamente en CLEAN.

**Pipeline CLEAN usa SQL puro (psycopg2 con autocommit).** Con 79M filas, traer datos
a Python causa crashes de memoria. El `INSERT INTO ... SELECT` dentro de PostgreSQL
procesa cada mes sin ninguna fila en RAM de Python.

**`columnas_base` reemplazado por `reindex`.** El primer mes de 2023 tiene columnas
distintas a los meses posteriores. En lugar de hardcodear la lista de columnas, el
primer archivo exitoso define el esquema y `reindex` alinea los demás automáticamente.

**`dim_location` como role-playing dimension.** Una sola tabla de ubicaciones sirve
para `pickup_location_id` y `dropoff_location_id` en `fact_viajes`. Esto evita
duplicar 265 zonas TLC y es el patrón estándar de Kimball para dimensiones con
múltiples roles.

**Secrets, no hardcoding.** Todas las credenciales de PostgreSQL se gestionan
mediante el sistema de Secrets de Mage, referenciadas en `io_config.yaml` con
`mage_secret_var()`. El `.env` es la única fuente de verdad para credenciales y
no debe subirse al repositorio.

---

## Reglas de calidad aplicadas en la capa CLEAN

| Regla | Criterio |
|---|---|
| Duración válida | Entre 1 y 300 minutos |
| Distancia válida | Entre 0.1 y 200 millas |
| Tarifa base válida | Entre $1 y $1,000 |
| Total > 0 | `total_amount > 0` |
| Pasajeros válidos | Entre 1 y 8 |
| Dropoff posterior a pickup | `dropoff_datetime > pickup_datetime` |
| Fechas no nulas | Se eliminan registros sin datetime |
| Sin duplicados | `DROP DUPLICATES` por registro completo |
| Nulos numéricos → 0 | extra, mta_tax, tip_amount, tolls_amount, etc. |

Retención promedio observada: ~90% de los registros raw pasan a clean.
