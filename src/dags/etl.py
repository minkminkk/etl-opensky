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
    # Source is about 2 months behind current time therefore 
    # lag current time by 2 months
    interval_start = "{{ \
        data_interval_start \
        .to_date_string() \
    }}"
    interval_end = "{{ \
        data_interval_end \
        .to_date_string() \
    }}"

    extract = SparkSubmitOperator(
        task_id = "extract_opensky_api",
        application = "/opt/airflow/jobs/extract.py",
        application_args = [
            "EDDF", 
            interval_start,
            interval_end
        ],
        py_files = "/dist/spark-jobs*.egg",
    )