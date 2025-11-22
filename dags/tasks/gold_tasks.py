# dags/tasks/gold_tasks.py
from airflow.decorators import task
import logging

@task()
def aggregate_silver_to_gold(
    table_name: str,
    silver_s3_path: str,
    s3_bucket: str,
    to_folder: str,
    catalog_db: str
    ) -> str:


    # load data from silver layer to gold schema
    logger = logging.getLogger("airflow.task")
    gold_base_path = f"s3://{s3_bucket}/{to_folder}/{table_name}/"
    
    try:
        import awswrangler as wr

        logger.info(f"Getting {table_name} from silver schema")
        if wr.s3.list_objects(silver_s3_path):
            df = wr.s3.read_parquet(silver_s3_path)
        else:
            logger.warning(f"No raw data found at {silver_s3_path}")
            return gold_base_path

        agg = (
            df.groupby(["store_id", "product_id"])
            .agg(
                total_sales=("unit_price", lambda x: round(x.sum(), 2)),
                total_qty=("quantity", "sum"))
            .reset_index()
            )

        wr.s3.to_parquet(
            df = agg,
            path = gold_base_path,
            dataset = True,
            mode = "overwrite",
            database = catalog_db,
            table = f"gold_{table_name}"
        )

        logger.info(f"loaded {len(agg)} to {gold_base_path}")
        return gold_base_path

    except Exception as e:
        logger.error(f"Error loading {table_name} from silver to gold schema: {str(e)}", exc_info=True)
        raise
