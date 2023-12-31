# Setups
airflow_dir = ./containers/airflow
airflow_log_dir = $(airflow_dir)/logs
data_dir = $(airflow_dir)/data
repo_folders:
	mkdir -p $(airflow_log_dir) $(data_dir) \
		&& sudo chmod a+rw -R $(airflow_log_dir) $(data_dir)
download_data: repo_folders
	bash ./scripts/download_data.sh $(data_dir)
hdfs_folders:
	docker exec etl-opensky-hdfs-namenode-1 hdfs dfs \
		-mkdir -p /data_lake/airports /data_lake/flights /data_warehouse
setup: repo_folders download_data
	
# pack_job_deps:
# 	bash scripts/pack_job_deps.sh

# Docker-compose related
up:
	docker compose up -d
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop

# HDFS-related
purge-fs:
	docker exec etl-opensky-hdfs-namenode-1 hadoop fs -ls -C / | xargs /opt/hadoop/bin/hadoop fs -rm -R

# DAG-related
execution_date = 2018-01-01T00:00:00+00:00
dag_id = flights_daily
dag_run_id = manual__$(execution_date)

clear_states:
	docker exec etl-opensky-airflow-1 airflow tasks clear -y $(dag_id)
run_dag:
	docker exec etl-opensky-airflow-1 airflow dags test flights_daily $(execution_date)
run_extract_flights:
	docker exec etl-opensky-airflow-1 airflow tasks run flights_daily extract_flights $(dag_run_id)
run_extract_airports:
	docker exec etl-opensky-airflow-1 airflow tasks run flights_daily extract_airports $(dag_run_id)
run_load_dim_dates:
	docker exec etl-opensky-airflow-1 airflow tasks run flights_daily load_dim_dates $(dag_run_id)
