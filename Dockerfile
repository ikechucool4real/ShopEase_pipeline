FROM apache/airflow:3.1.0

USER root

COPY requirements.txt .

USER airflow

RUN pip install --no-cache-dir -r requirements.txt
