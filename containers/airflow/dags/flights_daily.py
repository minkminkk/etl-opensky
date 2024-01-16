from airflow import DAG
from airflow.decorators.task_group import task_group
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator
from airflow.exceptions import AirflowSkipException

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


    def upload_to_data_lake(
            local_path: str, 
            hdfs_path: str, 
            skips: bool = False
        ) -> None:
        """Upload file from local to data lake. Skip if remote file exists."""
        # Manual skip
        if skips:
            raise AirflowSkipException
        
        # Actual implementation with skip based on condition
        from utils import webhdfs

        logging.info("Uploading data from local to data lake.")
        with open(local_path, "rb") as file:
            try:
                webhdfs.upload(file, hdfs_path, overwrite = False)
            except FileExistsError:
                raise AirflowSkipException


    @task_group(
        group_id = "ingest_from_local",
        prefix_group_id = False,
        default_args = {"python_callable": upload_to_data_lake}
    )   # Visual grouping purpose
    def upload_local():
        from utils import webhdfs

        hdfs_dir_path = "/data_lake"
        cur_files = webhdfs.list_files(hdfs_dir_path)
        file_task_id_template = {
            "airports.json": "airports",
            "aircraft-database-complete-2024-01.csv": "aircrafts",
            "doc8643AircraftTypes.csv": "aircraft_types",
            "doc8643Manufacturers.csv": "manufacturers"
        }

        for file in file_task_id_template.keys():
            task_id_templated = file_task_id_template[file]
            params = {
                "local_path": f"/data/{file}",
                "hdfs_path": f"/data_lake/{file}",
                "skips": file in cur_files
            }
            
            PythonOperator(
                task_id = f"upload_{task_id_templated}_data", 
                op_kwargs = params
            )
    upload_local()


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
    