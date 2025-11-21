# dags/tasks/silver_tasks.py
from airflow.decorators import task

import logging

@task()
def load_bronze_to_silver(
    table_name: str,
    table_id: str,
    updated_at_exists: bool, 
    bronze_s3_path: str,
    s3_bucket: str,
    to_folder: str,
    catalog_db: str,
    append_only: str
    ) -> str:


    # load data from bronze layer to silver schema
    logger = logging.getLogger("airflow.task")
    try:
        import awswrangler as wr
        import pandas as pd
        wr.config.distributed = False
        
        logger.info(f"Getting {table_name} from silver schema")
        silver_s3_path = f"s3://{s3_bucket}/{to_folder}/{table_name}/"

        if wr.s3.does_object_exist(silver_s3_path):
            silver_df = wr.s3.read_parquet(silver_s3_path)
        else:
            silver_df = pd.DataFrame()
        
        logger.info("Getting new data from bronze schema")
        new_df = wr.s3.read_parquet(bronze_s3_path)

        logger.info("merging old and new data")
        if updated_at_exists:
            merged_df = pd.concat([silver_df, new_df])
            if not append_only:
                merged_df = merged_df.sort_values("updated_at").drop_duplicates(subset=[table_id], keep="last")
        else:
            merged_df = new_df

        wr.s3.to_parquet(
            df = merged_df,
            path = silver_s3_path,
            dataset = True,
            mode = "overwrite",
            database = catalog_db,
            table = f"silver_{table_name}"
        )

        logger.info(f"loaded {table_name} to silver schema")
        return silver_s3_path

    except Exception as e:
        logger.error(f"Error loading {table_name} from bronze to silver schema: {str(e)}", exc_info=True)
        raise
