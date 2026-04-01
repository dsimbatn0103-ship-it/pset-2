if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

@transformer
def transform(reporte, *args, **kwargs):
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
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    cfg         = ConfigFileLoader(config_path, 'default')

    with Postgres.with_config(cfg) as pg:
    # Contar filas totales en raw
        total = pg.load("SELECT COUNT(*) AS n FROM raw.viajes_taxi_amarillo")
        n_total = total['n'].iloc[0]
        print(f"Total filas en raw.viajes_taxi_amarillo: {n_total:,}")

        # Distribución por año/mes
        dist = pg.load("""
            SELECT
                EXTRACT(YEAR  FROM tpep_pickup_datetime)::int AS anio,
                EXTRACT(MONTH FROM tpep_pickup_datetime)::int AS mes,
                COUNT(*) AS viajes
            FROM raw.viajes_taxi_amarillo
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)
        print("\nDistribución en raw:")
        print(dist.to_string())

        # Porcentaje de nulos en columnas clave
        nulos = pg.load("""
            SELECT
                COUNT(*)                                       AS total,
                SUM(CASE WHEN tpep_pickup_datetime  IS NULL THEN 1 ELSE 0 END) AS nulos_pickup,
                SUM(CASE WHEN tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS nulos_dropoff,
                SUM(CASE WHEN trip_distance          IS NULL THEN 1 ELSE 0 END) AS nulos_dist,
                SUM(CASE WHEN total_amount           IS NULL THEN 1 ELSE 0 END) AS nulos_total
            FROM raw.viajes_taxi_amarillo
        """)
        print("\nNulos en columnas clave:")
        print(nulos.to_string())

    # Enriquecer el reporte con los datos de raw
    reporte['validado'] = True
    
    return reporte



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
