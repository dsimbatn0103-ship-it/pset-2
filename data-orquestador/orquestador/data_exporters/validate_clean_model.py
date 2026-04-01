if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import os
import psycopg2
import pandas as pd

def _get_conn():
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'data-warehouse'),
        port=int(os.environ.get('POSTGRES_PORT', 5432)),
        dbname=os.environ.get('POSTGRES_DB', 'warehouse'),
        user=os.environ.get('POSTGRES_USER', 'root'),
        password=os.environ.get('POSTGRES_PASSWORD', 'root'),
    )
    conn.autocommit = True
    return conn


def _crear_dims_faltantes(conn):
    """
    Crea las dimensiones que no existan todavía.
    dim_location es la que más frecuentemente falta.
    """
    with conn.cursor() as cur:

        # dim_vendor
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_vendor (
                vendor_id     INTEGER PRIMARY KEY,
                vendor_nombre TEXT NOT NULL
            );
            INSERT INTO clean.dim_vendor VALUES
                (0,'Desconocido'),
                (1,'Creative Mobile Technologies'),
                (2,'VeriFone Inc')
            ON CONFLICT DO NOTHING;
        """)

        # dim_payment_type
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_payment_type (
                payment_id     INTEGER PRIMARY KEY,
                payment_nombre TEXT NOT NULL
            );
            INSERT INTO clean.dim_payment_type VALUES
                (0,'No especificado'),(1,'Tarjeta de crédito'),
                (2,'Efectivo'),(3,'Sin cargo'),
                (4,'Disputa'),(5,'Desconocido'),(6,'Viaje cancelado')
            ON CONFLICT DO NOTHING;
        """)

        # dim_ratecode
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_ratecode (
                ratecode_id          INTEGER PRIMARY KEY,
                ratecode_descripcion TEXT NOT NULL
            );
            INSERT INTO clean.dim_ratecode VALUES
                (0,'No especificado'),(1,'Tarifa estándar NYC'),
                (2,'JFK'),(3,'Newark'),(4,'Nassau o Westchester'),
                (5,'Tarifa negociada'),(6,'Viaje grupal')
            ON CONFLICT DO NOTHING;
        """)

        # dim_location — la que faltaba
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean.dim_location (
                location_id INTEGER PRIMARY KEY,
                borough     TEXT,
                zone        TEXT
            );
            INSERT INTO clean.dim_location (location_id)
            SELECT DISTINCT loc_id FROM (
                SELECT pulocationid AS loc_id
                FROM raw.viajes_taxi_amarillo
                WHERE pulocationid IS NOT NULL
                UNION
                SELECT dolocationid
                FROM raw.viajes_taxi_amarillo
                WHERE dolocationid IS NOT NULL
            ) sub
            ON CONFLICT DO NOTHING;
        """)

    print("Dimensiones verificadas/creadas")


def _contar_tabla(cur, tabla: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {tabla};")
    return cur.fetchone()[0]


@data_exporter
def export_data(reporte, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    conn = _get_conn()

    try:
        # Garantizar que todas las dimensiones existen
        _crear_dims_faltantes(conn)

        with conn.cursor() as cur:

            # ── Conteo de todas las tablas ────────────────────────
            tablas = [
                'clean.fact_viajes',
                'clean.dim_vendor',
                'clean.dim_payment_type',
                'clean.dim_ratecode',
                'clean.dim_location',
                'clean.dim_fecha',
            ]
            print("=== Conteo de tablas CLEAN ===")
            conteos = {}
            for t in tablas:
                n = _contar_tabla(cur, t)
                conteos[t] = n
                print(f"  {t:<30}: {n:>15,}")

            # ── Métricas generales de fact_viajes ─────────────────
            cur.execute("""
                SELECT
                    MIN(pickup_datetime)                          AS fecha_min,
                    MAX(pickup_datetime)                          AS fecha_max,
                    COUNT(*)                                      AS total_viajes,
                    ROUND(AVG(total_amount)::numeric,        2)  AS tarifa_promedio,
                    ROUND(AVG(duracion_minutos)::numeric,    1)  AS duracion_prom_min,
                    ROUND(AVG(trip_distance)::numeric,       2)  AS distancia_prom,
                    ROUND(SUM(total_amount)::numeric,        0)  AS ingreso_total
                FROM clean.fact_viajes;
            """)
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            print("\n=== Métricas generales ===")
            for col, val in zip(cols, row):
                print(f"  {col:<25}: {val}")

            # ── Integridad referencial ────────────────────────────
            checks = [
                ("Viajes sin fecha",
                 "SELECT COUNT(*) FROM clean.fact_viajes f "
                 "LEFT JOIN clean.dim_fecha d ON d.fecha_id = f.fecha_id "
                 "WHERE d.fecha_id IS NULL"),
                ("Viajes sin vendor",
                 "SELECT COUNT(*) FROM clean.fact_viajes f "
                 "LEFT JOIN clean.dim_vendor v ON v.vendor_id = f.vendor_id "
                 "WHERE v.vendor_id IS NULL"),
                ("Viajes sin payment",
                 "SELECT COUNT(*) FROM clean.fact_viajes f "
                 "LEFT JOIN clean.dim_payment_type p ON p.payment_id = f.payment_id "
                 "WHERE p.payment_id IS NULL"),
            ]
            print("\n=== Integridad referencial ===")
            for nombre, q in checks:
                cur.execute(q)
                n = cur.fetchone()[0]
                estado = "OK" if n == 0 else f"ALERTA — {n:,} huérfanos"
                print(f"  {nombre:<25}: {estado}")

            # ── Comparativa raw vs clean por mes ──────────────────
            cur.execute("""
                SELECT
                    d.anio,
                    d.mes,
                    COUNT(*) AS viajes_clean
                FROM clean.fact_viajes f
                JOIN clean.dim_fecha   d ON d.fecha_id = f.fecha_id
                GROUP BY d.anio, d.mes
                ORDER BY d.anio, d.mes;
            """)
            rows  = cur.fetchall()
            cols2 = [d[0] for d in cur.description]
            df_dist = pd.DataFrame(rows, columns=cols2)
            print("\n=== Distribución por mes en CLEAN ===")
            print(df_dist.to_string(index=False))

            # ── Consulta analítica de muestra ─────────────────────
            cur.execute("""
                SELECT
                    d.anio,
                    d.mes,
                    v.vendor_nombre,
                    COUNT(*)                              AS viajes,
                    ROUND(AVG(f.total_amount)::numeric, 2) AS tarifa_prom
                FROM clean.fact_viajes     f
                JOIN clean.dim_fecha       d ON d.fecha_id  = f.fecha_id
                JOIN clean.dim_vendor      v ON v.vendor_id = f.vendor_id
                GROUP BY d.anio, d.mes, v.vendor_nombre
                ORDER BY d.anio, d.mes
                LIMIT 12;
            """)
            rows3 = cur.fetchall()
            cols3 = [d[0] for d in cur.description]
            df_analitica = pd.DataFrame(rows3, columns=cols3)
            print("\n=== Muestra analítica (JOIN fact + dims) ===")
            print(df_analitica.to_string(index=False))

    finally:
        conn.close()

    return reporte
