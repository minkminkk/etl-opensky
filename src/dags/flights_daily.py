from airflow import DAG
from airflow.decorators.task_group import task_group
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.exceptions import AirflowSkipException

import pendulum
import logging

from configs import GeneralConfig, ServiceConfig, AirflowPaths
GENERAL_CONF = GeneralConfig()  # airport_icao, date_format
HDFS_CONF = ServiceConfig("hdfs")
SPARK_CONF = ServiceConfig("spark")
AIRFLOW_PATHS = AirflowPaths()


# DAG's default arguments
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
    # Get template fields
    ds = "{{ ds }}"
    start_ts = "{{ data_interval_start.int_timestamp }}"
    end_ts = "{{ data_interval_end.int_timestamp }}"

    # Hook to HDFS through Airflow WebHDFSHook
    webhdfs_hook = WebHDFSHook()

    # Default args for SparkSubmitOperators
    default_py_files = f"{AIRFLOW_PATHS.config}/configs.py"


    """Extract flights data from OpenSky API and ingest into data lake"""
    ingest_flights = SparkSubmitOperator(
        task_id = "ingest_flights",
        name = "Extract flights data from OpenSky API into data lake",
        application = f"{AIRFLOW_PATHS.jobs}/extract_flights.py",
        application_args = [
            GENERAL_CONF.airport_icao, start_ts, end_ts
        ],
        py_files = default_py_files,
        retries = 5,
        retry_delay = 10
    )


    """Upload files from local to data lake"""
    @task_group(
        group_id = "upload_from_local"
    )
    def upload_local():
        # Add skip functionality for DAG logic and monitoring
        # Skipped -> File already exists
        def _upload(local_path: str, hdfs_path: str, skips: bool) -> None:
            if skips:
                raise AirflowSkipException
            
            webhdfs_hook.load_file(local_path, hdfs_path)

        file_task_id_template = {
            "airports.json": "airports",
            "airlines.json": "airlines",
            "aircraft-database-complete-2024-01.csv": "aircrafts",
            "doc8643AircraftTypes.csv": "aircraft_types",
            "doc8643Manufacturers.csv": "manufacturers"
        }   # mapping filenames to task id template

        for file in file_task_id_template.keys():
            local_path = f"/data/{file}"
            hdfs_path = f"/data_lake/{file}"
            
            params = {
                "local_path": local_path,
                "hdfs_path": hdfs_path,
                "skips": webhdfs_hook.check_for_path(hdfs_path)
            }
            task_id_templated = file_task_id_template[file]

            PythonOperator(
                task_id = task_id_templated,
                python_callable = _upload, 
                op_kwargs = params
            )


    """Create Hive tables in data warehouse"""
    create_hive_tbls = SparkSubmitOperator(
        task_id = "create_hive_tbls",
        name = "Create Hive tables in data warehouse",
        application = f"{AIRFLOW_PATHS.jobs}/create_hive_tbls.py",
        py_files = default_py_files
    )


    """Transform and load dimension tables to data warehouse"""
    @task_group(
        group_id = "load_dim_tables",
        default_args = {
            "trigger_rule": "none_failed",
            "py_files": default_py_files
        }
    )
    def load_dim_tables():
        SparkSubmitOperator(
            task_id = "airports",
            name = "Load airports dim table to data warehouse",
            application = f"{AIRFLOW_PATHS.jobs}/load_dim_airports.py"
        )
        SparkSubmitOperator(
            task_id = "aircrafts",
            name = "Load aircrafts dim table to data warehouse",
            application = f"{AIRFLOW_PATHS.jobs}/load_dim_aircrafts.py",
            application_args = [ds],
        )
        SparkSubmitOperator(
            task_id = "dates",
            name = "Prepopulate dates dim table to data warehouse",
            application = f"{AIRFLOW_PATHS.jobs}/load_dim_dates.py",
            application_args = ["2018-01-01", "2028-01-01"],
        )


    """Transform and load fact table to data warehouse"""
    load_fct_flights = SparkSubmitOperator(
        task_id = "load_fct_flights",
        name = "Transform and load flights data to data warehouse",
        application = f"{AIRFLOW_PATHS.jobs}/load_fct_flights.py",
        application_args = [ds],
        py_files = default_py_files
    )


    """Task dependencies"""
    [upload_local(), create_hive_tbls] >> load_dim_tables() >> load_fct_flights
    ingest_flights >> load_fct_flights