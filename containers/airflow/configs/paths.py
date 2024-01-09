"""Contains information about paths/URLs for DAG reference"""

import os

# Airflow paths
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
if not AIRFLOW_HOME:
    raise EnvironmentError("AIRFLOW_HOME is not assigned")

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DIR_DAGS = os.path.join(AIRFLOW_HOME, "dags")
DIR_JOBS = os.path.join(DIR_DAGS, "jobs")

# HDFS paths/URIs
NAMENODE_HOSTNAME = "namenode"
NAMENODE_PORT_IPC = 8020
NAMENODE_PORT_HTTP = 9870
HDFS_URI_PREFIX = f"hdfs://{NAMENODE_HOSTNAME}:{NAMENODE_PORT_IPC}"
WEBHDFS_URL_PREFIX = f"http://{NAMENODE_HOSTNAME}:{NAMENODE_PORT_HTTP}/webhdfs/v1"

# Spark paths/URIs
SPARK_MASTER_NAME = "spark-master"
SPARK_MASTER_PORT = 7077
SPARK_MASTER_COMMON = f"{SPARK_MASTER_NAME}:{SPARK_MASTER_PORT}"
SPARK_MASTER_URI = f"spark://{SPARK_MASTER_COMMON}"
SPARK_SQL_WAREHOUSE_DIR = f"{HDFS_URI_PREFIX}/data_warehouse"