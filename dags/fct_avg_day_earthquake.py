import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "d.lyshtva"
DAG_ID = "fct_avg_day_earthquake"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_avg_day_earthquake"

PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Europe/Madrid"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["dm", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_minio_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  
        poke_interval=60,
    )

    create_stg_if_not_exists = SQLExecuteQueryOperator(
        task_id="create_stg_if_not_exists",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS stg;
        """,
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT
            time::date AS date,
            avg(mag::float)
        FROM
            ods.fct_earthquake
        WHERE
            time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY 1
        """,
    )

    create_fct_avg_table_if_not_exists = SQLExecuteQueryOperator(
        task_id="create_fct_avg_table_if_not_exists",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA};
        CREATE TABLE IF NOT EXISTS {SCHEMA}.fct_avg_day_earthquake (
        date DATE,
        avg FLOAT
        )
        """,
    )

    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN
        (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        )
        """,
    )

    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
            start >>
            sensor_on_raw_layer >>
            create_stg_if_not_exists >>
            drop_stg_table_before >>
            create_stg_table >>
            create_fct_avg_table_if_not_exists >>
            drop_from_target_table >>
            insert_into_target_table >>
            drop_stg_table_after >>
            end
    )