# dags/main.py
from airflow.decorators import dag
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import logging
import os
import json
from tasks.raw_tasks import extract_postgresql_to_s3
from tasks.bronze_tasks import load_raw_to_bronze
from tasks.silver_tasks import load_bronze_to_silver
from tasks.gold_tasks import aggregate_silver_to_gold

# Setting basic configuration
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s [%(levelname)s] - %(message)s"
)

# Loading environment variables
base_dir = Path(__file__).resolve().parents[2]
load_dotenv(base_dir/'.env')

tables = json.loads(os.getenv("tables", "[]"))
tables_with_updated_at = json.loads(os.getenv("tables_with_updated_at", "[]"))
tables_with_append_only = json.loads(os.getenv("tables_with_append_only", "[]"))
table_in_gold = json.loads(os.getenv("table_in_gold", "[]"))
postgres_conn = json.loads(os.getenv("postgres_conn", "{}"))
id_map = json.loads(os.getenv("id_map", "{}"))
postgres_conn_uri = os.getenv("postgres_conn_uri")
s3_bucket = os.getenv("s3_bucket")
raw_path = os.getenv("raw_path")
bronze_path = os.getenv("bronze_path")
silver_path = os.getenv("silver_path")
gold_path = os.getenv("gold_path")
catalog_db = os.getenv("catalog_db")

# Defining Airflow DAG
default_args = {
    "owner": "airflow", 
    "retries": 1
    }

@dag(
    dag_id="etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 11, 21),
    schedule="@daily",
    catchup=False,
    tags=["postgres", "s3", "pipeline"]
)
def full_pipeline():
    run_date = "{{ ds }}"
    prev_run_date = "{{ prev_ds | default(None) }}"

    logger = logging.getLogger("airflow.task")
    logger.info(f"Extracting raw csv data and loading to s3")

    raw_tasks = {}
    for table in tables:
        raw_tasks[table] = extract_postgresql_to_s3.override(task_id=f"raw_{table}")(
            table_name=table,
            updated_at_exists=table in tables_with_updated_at,
            run_date=run_date,
            prev_run_date=prev_run_date,
            postgres_conn=postgres_conn,
            s3_bucket=s3_bucket,
            to_folder=raw_path
        )
    logger.info(f"Extracted {tables} from postgresql and loading to {raw_tasks[table]}")
    
    logger.info(f"loading data from raw to bronze")
    bronze_tasks = {}
    for table in tables:
        bronze_tasks[table] = load_raw_to_bronze.override(task_id=f"bronze_{table}")(
            table_name=table,
            run_date=run_date,
            s3_bucket=s3_bucket,
            raw_s3_path=raw_tasks[table],
            to_folder=bronze_path,
            catalog_db=catalog_db
        )
        raw_tasks[table] >> bronze_tasks[table]
    logger.info(f"Extracted {tables} from raw and loading to {bronze_tasks[table]}")

    logger.info(f"loading data from bronze from silver")
    silver_tasks = {}
    for table in tables:
        silver_tasks[table] = load_bronze_to_silver.override(task_id=f"silver_{table}")(
            table_name=table,
            table_id=id_map[table],
            updated_at_exists=table in tables_with_updated_at,
            s3_bucket=s3_bucket,
            bronze_s3_path=bronze_tasks[table],
            to_folder=silver_path,
            catalog_db=catalog_db,
            append_only=table in tables_with_append_only
        )
        bronze_tasks[table] >> silver_tasks[table]
    logger.info(f"Extracted {tables} from bronze and loading to {silver_tasks[table]}")

    logger.info(f"loading data from silver to gold")
    gold_tasks = {}
    for table in table_in_gold:  
        gold_tasks[table] = aggregate_silver_to_gold.override(task_id=f"gold_{table}")(
            table_name=table,
            silver_s3_path=silver_tasks[table],
            s3_bucket=s3_bucket,
            to_folder=gold_path,
            catalog_db=catalog_db
        )
        silver_tasks[table] >> gold_tasks[table]
    logger.info(f"Data loaded to gold")


pipeline_dag = full_pipeline()
