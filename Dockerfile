# Capa 1: imagen base
FROM python:3.10-slim

# Configurar la carpeta (directorio) de trabajo
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ingest-data.py /app/

CMD ["python", "ingest-data.py"]