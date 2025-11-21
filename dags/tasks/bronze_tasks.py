# dags/tasks/bronze_tasks.py
from airflow.decorators import task
import logging

@task()
def load_raw_to_bronze(
    table_name: str, 
    run_date: str, 
    s3_bucket: str,
    raw_s3_path: str,
    to_folder: str,
    catalog_db:str
    ) -> str:


    # load data from raw layer to bronze schema
    logger = logging.getLogger("airflow.task")
    try:
        import awswrangler as wr
        wr.config.distributed = False
        
        logger.info(f"Getting {table_name} from raw schema")
        df = wr.s3.read_csv(raw_s3_path)

        bronze_s3_path = f"s3://{s3_bucket}/{to_folder}/{table_name}/updated_at={run_date}/"

        wr.s3.to_parquet(
            df = df,
            path = bronze_s3_path,
            dataset = True,
            mode = "overwrite",
            database = catalog_db,
            table = f"bronze_{table_name}"
        )

        logger.info(f"loaded {table_name} to bronze {bronze_s3_path}")
        return bronze_s3_path

    except Exception as e:
        logger.error(f"Error loading {table_name} from raw to bronze schema: {str(e)}", exc_info=True)
        raise
