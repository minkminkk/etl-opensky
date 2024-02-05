# etl-opensky

## 1. About this project

> Scenario: You work as a data engineer for the [Frankfurt Airport](https://en.wikipedia.org/wiki/Frankfurt_Airport). You are tasked to write an ETL pipeline to gather historical data about flights into & out of the airport into the data warehouse, from which business users can query and do further analysis.

This project aims to create an ETL pipeline that runs daily 

## 2. Technologies used

- **Apache Airflow** for data orchestration.
- **Apache Spark** for data transformation and query processing.
- **Apache Hadoop's HDFS** for distributed file system.
- **Apache Hive** as the data warehousing tool.

## 3. Additional information

### 3.1. Data flow

TODO

### 3.2. Airflow DAG

![Airflow DAG](imgs/airflow_dag.png)

- Notes:
    - `upload_from_local` task group will skip if files already uploaded.
    - `load_dim_tbls` task group has `none_failed` trigger rule (instead of the default `all_success`).

### 3.3. Data warehouse schema

![DWH Schema](imgs/dwh_schema.png)

## 4. Installation

### 4.1. Set up

1. Clone this repository and navigate to project directory:

```bash
git clone https://github.com/minkminkk/etl-opensky
cd etl-opensky
```

2. Run initial setup scripts through `Makefile`:

```bash
make setup
```

3. Build/get necessary images and start containers:

```bash
make up
```

After the containers have successfully started, the system is ready to use.

### 4.2. Tear down

After you are done and want to delete all containers:

```bash
make down
```

## 5. Getting started

### 5.1. Interfaces

- Airflow web UI: `localhost:8080`.
- Spark master web UI: `localhost:8081`.
- HDFS web UI: `localhost:9870`.
- Hiveserver web UI: `localhost:10002`.

### 5.2. Schedule DAGs

- Browser-based: Via the Airflow web UI using your browser.
- Command line-based:
```bash
make airflow_shell
```

> [!warning]
> The OpenSky API will mostly errors out when we tries to retrieve data near current time. Therefore be sure to only run pipelines about 2 months earlier than current date.

### 5.3. Query pipeline output in Data Warehouse

- Via Hive's command line `beeline`:
```bash
make beeline
```

The `Makefile` also connects to the existing database. After connected, it is ready to write queries using HQL.