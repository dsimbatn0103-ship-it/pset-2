if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
import psycopg2
import pandas as pd

def _get_conn():
    """
    Conexión psycopg2 directa leyendo variables de entorno
    que Docker Compose inyecta desde el .env.
    autocommit=True → cada statement hace commit inmediato,
    sin transacciones abiertas que acumulen memoria.
    """
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'data-warehouse'),
        port=int(os.environ.get('POSTGRES_PORT', 5432)),
        dbname=os.environ.get('POSTGRES_DB', 'warehouse'),
        user=os.environ.get('POSTGRES_USER', 'root'),
        password=os.environ.get('POSTGRES_PASSWORD', 'root'),
    )
    conn.autocommit = True   # ← clave: sin transacciones implícitas
    return conn

def _garantizar_infraestructura(conn):
    """Crea dim_fecha y fact_viajes si no existen. Sin datos en RAM."""
    with conn.cursor() as cur:

        # dim_fecha
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_fecha (
                fecha_id   SERIAL PRIMARY KEY,
                fecha      DATE NOT NULL UNIQUE,
                anio       INTEGER,
                mes        INTEGER,
                dia        INTEGER,
                dia_semana INTEGER,
                es_finde   BOOLEAN
            );
        """)
        cur.execute("""
            INSERT INTO clean.dim_fecha
                (fecha, anio, mes, dia, dia_semana, es_finde)
            SELECT DISTINCT
                tpep_pickup_datetime::date,
                EXTRACT(YEAR  FROM tpep_pickup_datetime)::int,
                EXTRACT(MONTH FROM tpep_pickup_datetime)::int,
                EXTRACT(DAY   FROM tpep_pickup_datetime)::int,
                EXTRACT(DOW   FROM tpep_pickup_datetime)::int,
                EXTRACT(DOW   FROM tpep_pickup_datetime) IN (0, 6)
            FROM raw.viajes_taxi_amarillo
            WHERE tpep_pickup_datetime BETWEEN '2023-01-01' AND '2024-12-31'
            ON CONFLICT (fecha) DO NOTHING;
        """)
        cur.execute("SELECT COUNT(*) FROM clean.dim_fecha;")
        n = cur.fetchone()[0]
        print(f"dim_fecha lista: {n} fechas")

        # fact_viajes
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.fact_viajes (
                viaje_id              BIGSERIAL PRIMARY KEY,
                fecha_id              INTEGER,
                vendor_id             INTEGER,
                pickup_location_id    INTEGER,
                dropoff_location_id   INTEGER,
                payment_id            INTEGER,
                ratecode_id           INTEGER,
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
        print("fact_viajes lista")


def _procesar_mes(conn, year: int, month: int) -> dict:
    fecha_ini = f"{year}-{month:02d}-01"
    fecha_fin = f"'{fecha_ini}'::date + interval '1 month'"

    with conn.cursor() as cur:

        cur.execute(f"""
            SELECT COUNT(*)
            FROM raw.viajes_taxi_amarillo
            WHERE tpep_pickup_datetime >= '{fecha_ini}'
              AND tpep_pickup_datetime  <  {fecha_fin};
        """)
        n_raw = cur.fetchone()[0]

        if n_raw == 0:
            return {'filas_raw': 0, 'filas_clean': 0,
                    'pct_retenido': 0.0, 'estado': 'SIN DATOS'}

        cur.execute(f"""
            INSERT INTO clean.fact_viajes (
                fecha_id, vendor_id,
                pickup_location_id, dropoff_location_id,
                payment_id, ratecode_id,
                passenger_count, trip_distance,
                fare_amount, extra, mta_tax, tip_amount,
                tolls_amount, improvement_surcharge,
                congestion_surcharge, airport_fee,
                total_amount, duracion_minutos,
                pickup_datetime, dropoff_datetime
            )
            SELECT
                df.fecha_id,
                COALESCE(r.vendorid::int,      0),
                r.pulocationid::int,
                r.dolocationid::int,
                COALESCE(r.payment_type::int,  0),
                COALESCE(r.ratecodeid::int,    0),
                r.passenger_count::int,
                ROUND(r.trip_distance::numeric,                      2),
                ROUND(r.fare_amount::numeric,                        2),
                ROUND(COALESCE(r.extra,                 0)::numeric, 2),
                ROUND(COALESCE(r.mta_tax,               0)::numeric, 2),
                ROUND(COALESCE(r.tip_amount,            0)::numeric, 2),
                ROUND(COALESCE(r.tolls_amount,          0)::numeric, 2),
                ROUND(COALESCE(r.improvement_surcharge, 0)::numeric, 2),
                ROUND(COALESCE(r.congestion_surcharge,  0)::numeric, 2),
                ROUND(COALESCE(r.airport_fee,           0)::numeric, 2),
                ROUND(r.total_amount::numeric,                       2),
                ROUND(
                    (EXTRACT(EPOCH FROM (
                        r.tpep_dropoff_datetime - r.tpep_pickup_datetime
                    )) / 60)::numeric
                , 2),
                r.tpep_pickup_datetime,
                r.tpep_dropoff_datetime
            FROM raw.viajes_taxi_amarillo r
            JOIN clean.dim_fecha df
              ON df.fecha = r.tpep_pickup_datetime::date
            WHERE
                r.tpep_pickup_datetime >= '{fecha_ini}'
                AND r.tpep_pickup_datetime  <  {fecha_fin}
                AND r.tpep_dropoff_datetime  >  r.tpep_pickup_datetime
                AND (EXTRACT(EPOCH FROM (
                        r.tpep_dropoff_datetime - r.tpep_pickup_datetime
                    )) / 60) BETWEEN 1 AND 300
                AND r.trip_distance    BETWEEN 0.1  AND 200
                AND r.fare_amount      BETWEEN 1    AND 1000
                AND r.total_amount     > 0
                AND r.passenger_count  BETWEEN 1    AND 8
                AND r.pulocationid     IS NOT NULL
                AND r.dolocationid     IS NOT NULL;
        """)

        n_clean = cur.rowcount

    pct = round(100 * n_clean / n_raw, 1) if n_raw > 0 else 0.0
    return {
        'filas_raw':    int(n_raw),
        'filas_clean':  int(n_clean),
        'pct_retenido': pct,
        'estado':       'OK',
    }

@transformer
def transform(resumen_dims, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    conn    = _get_conn()
    reporte = []

    try:
        _garantizar_infraestructura(conn)

        for year in [2023, 2024]:
            for month in range(1, 13):
                print(f"\n{'='*46}")
                print(f"Procesando {year}-{month:02d}")

                try:
                    res = _procesar_mes(conn, year, month)
                    res['periodo'] = f"{year}-{month:02d}"
                    reporte.append(res)

                    if res['estado'] == 'OK':
                        print(f"  Raw:      {res['filas_raw']:>12,}")
                        print(f"  Clean:    {res['filas_clean']:>12,}")
                        print(f"  Retenido: {res['pct_retenido']:>11.1f}%")
                    else:
                        print(f"  Estado:   {res['estado']}")

                except Exception as e:
                    print(f"  ERROR: {e}")
                    reporte.append({
                        'periodo': f"{year}-{month:02d}",
                        'filas_raw': 0, 'filas_clean': 0,
                        'pct_retenido': 0.0,
                        'estado': f'ERROR: {e}',
                    })

    finally:
        conn.close()   # siempre cerrar la conexión

    df = pd.DataFrame(reporte)
    print("\n=== Reporte final CLEAN ===")
    print(df.to_string())
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
