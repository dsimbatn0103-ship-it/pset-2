import pandas as pd
import sqlalchemy
import math
from tqdm.auto import tqdm
import traceback

def main():
    #Conexión a PostgreSQL
    conexion = sqlalchemy.create_engine(
        'postgresql://root:root@pset-2-data-warehouse-1:5432/warehouse'
    )

    # Definir las fechas de inicio y fin
    start_year = 2023
    start_month = 1
    end_year = 2024
    end_month = 12

    # Tamaño de chunk
    tamano = 100000

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_year and month > end_month:
                break

            URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
            print(f"\nDescargando datos: {year}-{month:02d}")
            
            try:
                # EXTRACT (sin alterar)
                datos_crudos = pd.read_parquet(URL)

                print(f"Filas descargadas: {datos_crudos.shape[0]}")
                
                # Nombre dinámico de tabla RAW (una por mes)
                nombre_tabla = f"raw_viajes_taxi_{year}_{month:02d}"

                # Crear tabla automáticamente según esquema del mes
                print(f"Creando tabla {nombre_tabla} en PostgreSQL...")

                datos_crudos.head(0).to_sql(
                    name='viajes_taxi_amarillo',
                    con=conexion,
                    if_exists='replace',
                    index=False
                )

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