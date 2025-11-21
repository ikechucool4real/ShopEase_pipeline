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
    try:
        import awswrangler as wr
        wr.config.distributed = False

        logger.info(f"Getting {table_name} from silver schema")
        df = wr.s3.read_parquet(silver_s3_path)

        agg = (
            df.groupby(["store_id", "product_id"])
            .agg(
                total_sales=("unit_price", lambda x: round(x.sum(), 2)),
                total_qty=("quantity", "sum"))
            .reset_index()
            )

        gold_s3_path = f"s3://{s3_bucket}/{to_folder}/{table_name}/"

        wr.s3.to_parquet(
            df = agg,
            path = gold_s3_path,
            dataset = True,
            mode = "overwrite",
            database = catalog_db,
            table = f"gold_{table_name}"
        )

        logger.info(f"loaded {table_name} to gold schema")
        return gold_s3_path

    except Exception as e:
        logger.error(f"Error loading {table_name} from silver to gold schema: {str(e)}", exc_info=True)
        raise
