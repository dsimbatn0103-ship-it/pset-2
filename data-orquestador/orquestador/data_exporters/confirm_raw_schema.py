if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import pandas as pd
from datetime import datetime
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path


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
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    cfg         = ConfigFileLoader(config_path, 'default')

    with Postgres.with_config(cfg) as pg:

        # Crear tabla de log si no existe
        pg.execute("""
            CREATE TABLE IF NOT EXISTS raw.log_carga (
                id          SERIAL PRIMARY KEY,
                ejecutado_en TIMESTAMP DEFAULT NOW(),
                anio        INTEGER,
                mes         INTEGER,
                filas       BIGINT,
                estado      TEXT
            );
        """)

        # Guardar el reporte de esta ejecución
        log = reporte[['anio', 'mes', 'filas', 'estado']].copy()
        log['ejecutado_en'] = datetime.utcnow()
        pg.export(log, schema_name='raw', table_name='log_carga',
                  if_exists='append', index=False)

        # Resumen final
        resumen = pg.load("""
            SELECT
                COUNT(*)           AS total_filas,
                MIN(tpep_pickup_datetime) AS fecha_min,
                MAX(tpep_pickup_datetime) AS fecha_max
            FROM raw.viajes_taxi_amarillo
        """)
        print("=== Schema RAW confirmado ===")
        print(resumen.to_string())

    return reporte


