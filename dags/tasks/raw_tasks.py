# dags/tasks/bronze_tasks.py
from airflow.decorators import task
import logging


@task()
def extract_postgresql_to_s3(
    table_name: str,
    partition_col_exists: bool, 
    run_date: str, 
    prev_run_date: str, 
    postgres_conn: dict, 
    s3_bucket: str,
    to_folder: str,
    partition_col: str
    ) -> str:

    # Extract data from postgreSQL to S3
    logger = logging.getLogger("airflow.task")
    base_s3_path = f"s3://{s3_bucket}/{to_folder}/{table_name}/" 
    s3_path = f"s3://{s3_bucket}/{to_folder}/{table_name}/run_date={run_date}/{table_name}_{run_date}.csv"

    try:
        import pandas as pd
        import awswrangler as wr
        import psycopg2

        logger.info(f"Extracting data from {table_name} in postgreSQL")
        with psycopg2.connect(
            dbname=postgres_conn["dbname"],
            user=postgres_conn["user"],
            password=postgres_conn["password"],
            host=postgres_conn["host"],
            port=postgres_conn["port"]
        ) as conn:
            if partition_col_exists:
                if prev_run_date is None or prev_run_date == "None":  
                    query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE {partition_col}::date <= '{run_date}'
                    """
                else:
                    query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE {partition_col}::date > '{prev_run_date}'
                    AND {partition_col}::date <= '{run_date}'
                    """
            else:
                query = f"""
                    SELECT *
                    FROM {table_name}
                """

            df = pd.read_sql(query, conn)
        logger.info(f"Extracted {len(df)} rows from {table_name}")
        
        if df.empty:
            logger.warning(f"No data found for {table_name} between {prev_run_date} and {run_date}")
            return base_s3_path
        
        
        wr.s3.to_csv(df=df, path=s3_path, index=False)
        logging.info(f"Uploaded raw data to {s3_path}")
        return s3_path

    except Exception as e:
        logger.error(f"Error extracting data from postgreSQL to S3: {str(e)}", exc_info=True)
        raise
