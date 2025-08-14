import logging
import pendulum
import duckdb
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

OWNER = "d.lyshtva"
DAG_ID = "row_from_api_to_minio"

LAYER = "raw"
SOURCE = "earthquake"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

args = {
    "owner": OWNER,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def get_and_transfer_api_data_to_minio(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")

    con = duckdb.connect()
    try:
        sql = f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;

        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY (
            SELECT *
            FROM read_csv_auto(
              'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}'
            )
        )
        TO 's3://prod/{LAYER}/{SOURCE}/{{{{ ds }}}}/part-000.parquet'
        WITH (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        con.sql(sql)
    finally:
        con.close()

    logging.info(f"✅ Ingest finished for {start_date}")


with DAG(
    dag_id=DAG_ID,
    description="Ingest raw earthquake data from public API to S3/MinIO as Parquet.",
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),
    schedule="0 5 * * *",
    catchup=True,
    default_args=args,
    tags=["s3", "raw"],
    max_active_tasks=1,
    max_active_runs=1,
    concurrency=1,
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_task = PythonOperator(
        task_id="get_and_transfer_api_data_to_minio",
        python_callable=get_and_transfer_api_data_to_minio,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest_task >> end
