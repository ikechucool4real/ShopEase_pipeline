# dags/tasks/bronze_tasks.py
from airflow.decorators import task
from datetime import datetime
import logging

@task()
def load_raw_to_bronze(
    table_name: str,
    run_date: str, 
    s3_bucket: str,
    raw_s3_path: str,
    to_folder: str,
    catalog_db:str,
    ) -> str:


    # load data from raw layer to bronze schema
    logger = logging.getLogger("airflow.task")
    bronze_base_path = f"s3://{s3_bucket}/{to_folder}/{table_name}/"        

    try:
        import awswrangler as wr
        
        logger.info(f"Getting {table_name} from raw schema")
        if wr.s3.does_object_exist(raw_s3_path):
            df = wr.s3.read_csv(raw_s3_path)
        else:
            logger.warning(f"No raw data found at {raw_s3_path}")
            return bronze_base_path
        
        df["ingest_timestamp"] = datetime.now()

        wr.s3.to_parquet(
            df = df,
            path = bronze_base_path,
            dataset = True,
            mode = "overwrite",
            database = catalog_db,
            table = f"bronze_{table_name}"
        )

        logger.info(f"loaded {len(df)} to {bronze_base_path}")
        return bronze_base_path

    except Exception as e:
        logger.error(f"Error loading {table_name} from raw to bronze schema: {str(e)}", exc_info=True)
        raise
