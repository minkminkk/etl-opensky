# Setups
airflow_dir=./containers/airflow
airflow_log_dir=./logs/airflow
hiveserver_log_dir=./logs/hiveserver
data_dir=./data
repo_folders:
	mkdir -p $(airflow_log_dir) $(hiveserver_log_dir) $(data_dir) \
		&& sudo chmod a+rw -R $(airflow_log_dir) $(hiveserver_log_dir) $(data_dir)
download_data: repo_folders
	bash ./scripts/download_data.sh $(data_dir)
setup: repo_folders download_data
	
# Docker-compose related
up: setup
	docker compose up -d
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop

# Make shells
airflow_shell:
	docker exec -it etl-opensky-airflow-1 /bin/bash
beeline:
	docker exec -it etl-opensky-hive-server-1 beeline -u 'jdbc:hive2://hive-server:10000'