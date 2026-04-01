if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import math
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

    BASE_URL   = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    YEARS      = [2023, 2024]
    CHUNK_SIZE = 100_000          # filas por escritura a Postgres
    reporte    = []

    SCHEMA_OBJETIVO = None       # se aprende del primer archivo exitoso
    tabla_creada    = False

    with Postgres.with_config(cfg) as pg:

        # Garantizar que el schema raw existe
        pg.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        for year in YEARS:
            for month in range(1, 13):

                url = BASE_URL.format(year=year, month=month)
                print(f"\nProcesando {year}-{month:02d}...")

                try:
                    # ── Leer el parquet del mes ───────────────────
                    df = pd.read_parquet(url)

                    # ── Normalizar nombres a snake_case ───────────
                    df.columns = (
                        df.columns.str.strip().str.lower()
                        .str.replace(' ', '_', regex=False)
                    )

                    # ── El primer mes define el esquema objetivo ──
                    if SCHEMA_OBJETIVO is None:
                        SCHEMA_OBJETIVO = list(df.columns)

                    # Alinear columnas para evitar errores de "column does not exist"
                    df = df.reindex(columns=SCHEMA_OBJETIVO)

                    # ── Tipado mínimo permitido en RAW ────────────
                    for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce')

                    # ── Columna de auditoría ──────────────────────
                    df['_anio'] = year
                    df['_mes']  = month

                    # ── Escribir en chunks ────────────────────────
                    with Postgres.with_config(cfg) as pg:
                        num_chunks = math.ceil(len(df) / CHUNK_SIZE)

                        for i in range(num_chunks):
                            # AQUÍ se define el chunk correctamente antes de usarlo
                            chunk = df.iloc[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]
                            
                            if chunk.empty:
                                continue

                            # Lógica de creación de tabla:
                            # Si es el primer éxito absoluto, 'replace'. De lo contrario, 'append'.
                            modo = 'replace' if (not tabla_creada) else 'append'

                            pg.export(
                                chunk,
                                schema_name='raw',
                                table_name='viajes_taxi_amarillo',
                                if_exists=modo,
                                index=False,
                            )
                            # Una vez que se exporta el primer chunk con éxito, la tabla existe
                            tabla_creada = True

                    print(f"  OK: {len(df)} filas escritas.")
                    reporte.append({'anio': year, 'mes': month, 'filas': len(df), 'estado': 'OK'})

                except Exception as e:
                    error_msg = str(e).split('\n')[0] # Error corto
                    print(f"  ERROR en {year}-{month}: {error_msg}")
                    reporte.append({'anio': year, 'mes': month, 'filas': 0, 'estado': f'ERROR: {error_msg}'})

                finally:
                    if 'df' in locals():
                        del df

        return pd.DataFrame(reporte)

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
