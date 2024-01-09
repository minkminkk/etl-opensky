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
    description = "Daily ETL pipeline for flights related to Frankfurt airport",
    default_args = default_args
) as dag:
    import logging
    from configs.paths import *
    from dags.utils import webhdfs

    ds = "{{ ds }}"     # DAG run date


    # extract_flights = SparkSubmitOperator(
    #     task_id = "extract_flights",
    #     application = f"{DIR_JOBS}/extract_flights.py",
    #     application_args = ["EDDF", ds],
    #     name = "Extract flights data",
    #     retries = 5,
    #     retry_delay = 10
    # )


    @task(task_id = "extract_airports")
    def extract_airports():
        # Check if file already exists in HDFS
        hdfs_path = "/data_lake/airports.json"
        if webhdfs.file_exists(hdfs_path):
            logging.info("File already exists in HDFS. Task finished.")
            return 0
        
        logging.info("Copying data from local to HDFS.")
        local_path = "/data/airports.json"
        webhdfs.create_file(local_path, hdfs_path, overwrite = True)
    ext_airports = extract_airports()


    # extract_airports = SparkSubmitOperator(
    #     task_id = "extract_airports",
    #     application = f"{DIR_JOBS}/extract_airports.py",
    #     name = "Extract airport data",
    #     files = "/data/airports.json"
    # )


    # transform = SparkSubmitOperator(
    #     task_id = "transform",
    #     application = f"{DIR_JOBS}/transform.py",
    #     application_args = [ds]
    # )


    # load_dim_dates = SparkSubmitOperator(
    #     task_id = "load_dim_dates",
    #     application = f"{DIR_JOBS}/load_dim_dates.py",
    #     application_args = ["2018-01-01", "2028-01-01"]
    # )


    # # TODO: Finished these tasks
    # # load_dim_airports
    # # extract_dim_aircraft


    # extract_flights >> transform