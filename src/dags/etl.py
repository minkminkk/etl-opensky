from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator
from datetime import datetime


default_args = {
    "start_date": datetime(2018, 1, 1),
    "end_date": datetime(2020, 1, 1),
    "schedule": "@daily",
    "retries": 0
}


with DAG(
    dag_id = "flights_daily",
    description = "Extract flights info from/to Frankfurt airport daily",
    default_args = default_args
) as dag:
    ds = "{{ ds }}"     # DAG run date 

    extract = SparkSubmitOperator(
        task_id = "extract_opensky_api",
        application = "/opt/airflow/jobs/extract.py",
        application_args = [
            "EDDF", 
            ds
        ],
        py_files = "/dist/spark-jobs*.egg",
        retries = 5,
        retry_delay = 10
    )

    transform = SparkSubmitOperator(
        task_id = "transform",
        application = "/opt/airflow/jobs/transform.py",
        application_args = [ds]
    )

    extract >> transform