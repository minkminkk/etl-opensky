from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator

import pendulum
import logging


default_args = {
    "start_date": pendulum.datetime(2018, 1, 1),
    "schedule": "@daily",
    "retries": 0
}


with DAG(
    dag_id = "flights_daily",
    description = "Daily ETL pipeline for flights related to Frankfurt airport",
    default_args = default_args
) as dag:
    from configs import GeneralConfig, AirflowConfig, \
        HDFSConfig, WebHDFSConfig, SparkConfig
    from utils import webhdfs

    # Get template fields
    ds = "{{ ds }}"
    start_ts = "{{ data_interval_start.int_timestamp }}"
    end_ts = "{{ data_interval_end.int_timestamp }}"

    # Get configs
    general_conf = GeneralConfig()  # airport_icao, date_format
    airflow_conf = AirflowConfig()
    hdfs_conf = HDFSConfig()
    webhdfs_conf = WebHDFSConfig()
    spark_conf = SparkConfig()

    # Default args for SparkSubmitOperators
    default_py_files = f"{airflow_conf.path.config}/configs.py"


    ingest_flights = SparkSubmitOperator(
        task_id = "ingest_flights",
        name = "Extract flights data from OpenSky API into data lake",
        application = f"{airflow_conf.path.jobs}/extract_flights.py",
        application_args = [
            general_conf.airport_icao, start_ts, end_ts
        ],
        py_files = default_py_files,
        retries = 5,
        retry_delay = 10
    )

    @task(task_id = "ingest_airports_from_local")
    def ingest_airports(hdfs_path: str):
        """Upload airports.json from local to HDFS. Skip if already uploaded."""
        
        # Check if file already exists in HDFS
        if webhdfs.file_exists(hdfs_path):
            logging.info("File already exists in HDFS. Task finished.")
            return 0
        
        # If not exists, upload file
        logging.info("Uploading data from local to HDFS.")
        local_path = "/data/airports.json"

        with open(local_path, "r") as file:
            webhdfs.upload(file, hdfs_path, overwrite = True)
    ingest_airports("/data_lake/airports.json")


    # extract_airports = SparkSubmitOperator(
    #     task_id = "extract_airports",
    #     application = f"{airflow_confs.jobs}/extract_airports.py",
    #     name = "Extract airport data",
    #     files = "/data/airports.json"
    # )


    # transform = SparkSubmitOperator(
    #     task_id = "transform",
    #     application = f"{airflow_conf.path.jobs}/transform.py",
    #     application_args = [ds]
    # )


    #TODO: Set up Hive service on cluster
    load_dim_dates = SparkSubmitOperator(
        task_id = "load_dim_dates",
        name = "Prepopulate dates dim table",
        application = f"{airflow_conf.path.jobs}/load_dim_dates.py",
        application_args = ["2018-01-01", "2028-01-01"],
        py_files = default_py_files
    )


    # # TODO: Finished these tasks
    # # load_dim_airports
    # # extract_dim_aircraft


    # extract_flights >> transform
    