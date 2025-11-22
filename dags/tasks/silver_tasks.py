# dags/tasks/silver_tasks.py
from airflow.decorators import task
from datetime import datetime
import logging

@task()
def load_bronze_to_silver(
    table_name: str,
    table_id: str,
    bronze_s3_path: str,
    s3_bucket: str,
    to_folder: str,
    partition_col_exists: bool,
    partition_col: str,
    catalog_db: str,
    append_only: bool
    ) -> str:


    # load data from bronze layer to silver schema
    logger = logging.getLogger("airflow.task")
    silver_base_path = f"s3://{s3_bucket}/{to_folder}/{table_name}/"
    
    try:
        import awswrangler as wr
        import pandas as pd

        logger.info("Getting new data from bronze schema")
        new_df = pd.DataFrame()  
        if wr.s3.list_objects(bronze_s3_path):
            new_df = wr.s3.read_parquet(bronze_s3_path)

        if new_df.empty:
            logger.warning(f"No new data found in {bronze_s3_path}")
            return silver_base_path
                    
        logger.info(f"Getting existing {table_name} from silver schema")
        if wr.s3.does_object_exist(silver_base_path):
            silver_df = wr.s3.read_parquet(silver_base_path)
        else:
            silver_df = pd.DataFrame()
        
        logger.info("merging existing and new data")
        merged_df = pd.concat([silver_df, new_df])
        if partition_col_exists and not append_only:
            merged_df = merged_df.sort_values(partition_col).drop_duplicates(subset=[table_id], keep="last")

        merged_df["ingest_timestamp"] = datetime.now()

        wr.s3.to_parquet(
            df = merged_df,
            path = silver_base_path,
            dataset = True,
            mode = "overwrite",
            database = catalog_db,
            table = f"silver_{table_name}"
        )

        logger.info(f"loaded {len(merged_df)} to {silver_base_path}")
        return silver_base_path

    except Exception as e:
        logger.error(f"Error loading {table_name} from bronze to silver schema: {str(e)}", exc_info=True)
        raise
