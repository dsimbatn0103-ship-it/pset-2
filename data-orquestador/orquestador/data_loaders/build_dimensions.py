if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    cfg         = ConfigFileLoader(config_path, 'default')

    with Postgres.with_config(cfg) as pg:

        # ── Schema clean ──────────────────────────────────────────
        pg.execute("CREATE SCHEMA IF NOT EXISTS clean;")

        # ── dim_vendor ────────────────────────────────────────────
        pg.execute("DROP TABLE IF EXISTS clean.dim_vendor CASCADE;")
        pg.execute("""
            CREATE TABLE clean.dim_vendor (
                vendor_id     INTEGER PRIMARY KEY,
                vendor_nombre TEXT NOT NULL
            );
        """)
        dim_vendor = pd.DataFrame([
            {'vendor_id': 0, 'vendor_nombre': 'Desconocido'},
            {'vendor_id': 1, 'vendor_nombre': 'Creative Mobile Technologies'},
            {'vendor_id': 2, 'vendor_nombre': 'VeriFone Inc'},
        ])
        pg.export(dim_vendor, schema_name='clean', table_name='dim_vendor',
                  if_exists='append', index=False)
        print(f"dim_vendor: {len(dim_vendor)} filas")

        # ── dim_payment_type ──────────────────────────────────────
        pg.execute("DROP TABLE IF EXISTS clean.dim_payment_type CASCADE;")
        pg.execute("""
            CREATE TABLE clean.dim_payment_type (
                payment_id     INTEGER PRIMARY KEY,
                payment_nombre TEXT NOT NULL
            );
        """)
        dim_payment = pd.DataFrame([
            {'payment_id': 0, 'payment_nombre': 'No especificado'},
            {'payment_id': 1, 'payment_nombre': 'Tarjeta de crédito'},
            {'payment_id': 2, 'payment_nombre': 'Efectivo'},
            {'payment_id': 3, 'payment_nombre': 'Sin cargo'},
            {'payment_id': 4, 'payment_nombre': 'Disputa'},
            {'payment_id': 5, 'payment_nombre': 'Desconocido'},
            {'payment_id': 6, 'payment_nombre': 'Viaje cancelado'},
        ])
        pg.export(dim_payment, schema_name='clean', table_name='dim_payment_type',
                  if_exists='append', index=False)
        print(f"dim_payment_type: {len(dim_payment)} filas")

        # ── dim_ratecode ──────────────────────────────────────────
        pg.execute("DROP TABLE IF EXISTS clean.dim_ratecode CASCADE;")
        pg.execute("""
            CREATE TABLE clean.dim_ratecode (
                ratecode_id          INTEGER PRIMARY KEY,
                ratecode_descripcion TEXT NOT NULL
            );
        """)
        dim_ratecode = pd.DataFrame([
            {'ratecode_id': 0, 'ratecode_descripcion': 'No especificado'},
            {'ratecode_id': 1, 'ratecode_descripcion': 'Tarifa estándar NYC'},
            {'ratecode_id': 2, 'ratecode_descripcion': 'JFK'},
            {'ratecode_id': 3, 'ratecode_descripcion': 'Newark'},
            {'ratecode_id': 4, 'ratecode_descripcion': 'Nassau o Westchester'},
            {'ratecode_id': 5, 'ratecode_descripcion': 'Tarifa negociada'},
            {'ratecode_id': 6, 'ratecode_descripcion': 'Viaje grupal'},
        ])
        pg.export(dim_ratecode, schema_name='clean', table_name='dim_ratecode',
                  if_exists='append', index=False)
        print(f"dim_ratecode: {len(dim_ratecode)} filas")

        # ── dim_location (IDs únicos de raw) ──────────────────────
        pg.execute("DROP TABLE IF EXISTS clean.dim_location CASCADE;")
        pg.execute("""
            CREATE TABLE clean.dim_location (
                location_id INTEGER PRIMARY KEY,
                borough     TEXT,
                zone        TEXT
            );
            INSERT INTO clean.dim_location (location_id)
            SELECT DISTINCT loc_id
            FROM (
                SELECT pulocationid AS loc_id
                FROM raw.viajes_taxi_amarillo
                WHERE pulocationid IS NOT NULL
                UNION
                SELECT dolocationid
                FROM raw.viajes_taxi_amarillo
                WHERE dolocationid IS NOT NULL
            ) s
            ORDER BY loc_id;
        """)
        n_loc = pg.load("SELECT COUNT(*) AS n FROM clean.dim_location")['n'].iloc[0]
        print(f"dim_location: {n_loc} filas")

        # ── dim_fecha (fechas únicas del periodo 2023-2024) ───────
        pg.execute("DROP TABLE IF EXISTS clean.dim_fecha CASCADE;")
        pg.execute("""
            CREATE TABLE clean.dim_fecha (
                fecha_id   SERIAL PRIMARY KEY,
                fecha      DATE NOT NULL UNIQUE,
                anio       INTEGER,
                mes        INTEGER,
                dia        INTEGER,
                dia_semana INTEGER,
                es_finde   BOOLEAN
            );
            INSERT INTO clean.dim_fecha (fecha, anio, mes, dia, dia_semana, es_finde)
            SELECT DISTINCT
                tpep_pickup_datetime::date,
                EXTRACT(YEAR  FROM tpep_pickup_datetime)::int,
                EXTRACT(MONTH FROM tpep_pickup_datetime)::int,
                EXTRACT(DAY   FROM tpep_pickup_datetime)::int,
                EXTRACT(DOW   FROM tpep_pickup_datetime)::int,
                EXTRACT(DOW   FROM tpep_pickup_datetime) IN (0, 6)
            FROM raw.viajes_taxi_amarillo
            WHERE tpep_pickup_datetime BETWEEN '2023-01-01' AND '2024-12-31'
            ORDER BY 1;
        """)
        n_fecha = pg.load("SELECT COUNT(*) AS n FROM clean.dim_fecha")['n'].iloc[0]
        print(f"dim_fecha: {n_fecha} filas")

        # ── fact_viajes vacía ─────────────────────────────────────
        pg.execute("DROP TABLE IF EXISTS clean.fact_viajes;")
        pg.execute("""
            CREATE TABLE clean.fact_viajes (
                viaje_id              BIGSERIAL PRIMARY KEY,
                fecha_id              INTEGER REFERENCES clean.dim_fecha(fecha_id),
                vendor_id             INTEGER REFERENCES clean.dim_vendor(vendor_id),
                pickup_location_id    INTEGER REFERENCES clean.dim_location(location_id),
                dropoff_location_id   INTEGER REFERENCES clean.dim_location(location_id),
                payment_id            INTEGER REFERENCES clean.dim_payment_type(payment_id),
                ratecode_id           INTEGER REFERENCES clean.dim_ratecode(ratecode_id),
                passenger_count       INTEGER,
                trip_distance         NUMERIC(8,2),
                fare_amount           NUMERIC(8,2),
                extra                 NUMERIC(8,2),
                mta_tax               NUMERIC(8,2),
                tip_amount            NUMERIC(8,2),
                tolls_amount          NUMERIC(8,2),
                improvement_surcharge NUMERIC(8,2),
                congestion_surcharge  NUMERIC(8,2),
                airport_fee           NUMERIC(8,2),
                total_amount          NUMERIC(8,2),
                duracion_minutos      NUMERIC(8,2),
                pickup_datetime       TIMESTAMP,
                dropoff_datetime      TIMESTAMP
            );
        """)
        print("fact_viajes: tabla creada vacía")

        # Retornar resumen de dimensiones
        resumen = pg.load("""
            SELECT 'dim_vendor'       AS tabla, COUNT(*) AS filas FROM clean.dim_vendor       UNION ALL
            SELECT 'dim_payment_type',           COUNT(*) FROM clean.dim_payment_type         UNION ALL
            SELECT 'dim_ratecode',               COUNT(*) FROM clean.dim_ratecode             UNION ALL
            SELECT 'dim_location',               COUNT(*) FROM clean.dim_location             UNION ALL
            SELECT 'dim_fecha',                  COUNT(*) FROM clean.dim_fecha
        """)
        return resumen


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
