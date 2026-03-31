import pandas as pd
import sqlalchemy
import math
from tqdm.auto import tqdm
import traceback

def main():
    #Conexión a PostgreSQL
    conexion = sqlalchemy.create_engine('postgresql://root:root@pset-2-data-warehouse-1:5432/warehouse')

    # Definir las fechas de inicio y fin
    start_year = 2023
    start_month = 1
    end_year = 2024
    end_month = 12

    # Tamaño de chunk
    tamano = 100000
    tabla_creada = False

    columnas_base = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee"  # clave del problema
    ]

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_year and month > end_month:
                break

            URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
            print(f"\nDescargando datos: {year}-{month:02d}")
            
            try:
                datos_crudos = pd.read_parquet(URL)

                print(f"Filas descargadas: {datos_crudos.shape[0]}")

                # NORMALIZAR COLUMNAS
                for col in columnas_base:
                    if col not in datos_crudos.columns:
                        datos_crudos[col] = None

                # Reordenar columnas
                datos_crudos = datos_crudos[columnas_base]

                # Crear tabla SOLO una vez
                if not tabla_creada:
                    print("Creando tabla en PostgreSQL...")
                    datos_crudos.head(0).to_sql(
                        name='viajes_taxi_amarillo',
                        con=conexion,
                        if_exists='replace',
                        index=False
                    )
                    tabla_creada = True

                # Calcular chunks
                num_chunks = math.ceil(len(datos_crudos) / tamano)

                print(f"Iniciando carga en chunks ({num_chunks} bloques)...")

                for i in tqdm(range(num_chunks)):

                    inicio = i * tamano
                    fin = inicio + tamano

                    chunk = datos_crudos.iloc[inicio:fin]

                    chunk.to_sql(
                        name='viajes_taxi_amarillo',
                        con=conexion,
                        if_exists='append',
                        index=False
                    )

                print(f"Mes {year}-{month:02d} cargado correctamente")

                # liberar memoria
                del datos_crudos

            except Exception as e:
                print(f" Error en {year}-{month:02d}: {e}")
                traceback.print_exc()

    print("\n Proceso finalizado correctamente")

# Verificamos que el archivo se ejecuta como principal
if __name__ == '__main__':
    main()