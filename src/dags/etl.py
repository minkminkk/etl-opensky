from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator
from datetime import datetime


default_args = {
    "start_date": datetime(2018, 1, 1),
    "schedule": "@daily",
    "retries": 0
}


with DAG(
    dag_id = "flights_daily",
    description = "Extract flights info from/to Frankfurt airport daily",
    default_args = default_args
) as dag:
    ds = "{{ ds }}"     # DAG run date 

    extract_flights = SparkSubmitOperator(
        task_id = "extract_flights",
        application = "/opt/airflow/jobs/extract_flights.py",
        application_args = ["EDDF", ds],
        retries = 5,
        retry_delay = 10
    )

    transform = SparkSubmitOperator(
        task_id = "transform",
        application = "/opt/airflow/jobs/transform.py",
        application_args = [ds]
    )

    load_dim_dates = SparkSubmitOperator(
        task_id = "load_dim_dates",
        application = "/opt/airflow/jobs/load_dim_dates.py",
        application_args = ["2018-01-01", "2028-01-01"]
    )

    # TODO: Finished these tasks
    # load_dim_airports
    # extract_dim_aircraft

    extract_flights >> transform