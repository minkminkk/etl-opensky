setup: download_data.sh
	bash ./scripts/download_data.sh \
		&& mkdir -p ./logs/spark ./logs/airflow \
		&& chmod a+rw -R ./logs

up:
	docker compose up -d
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop

purge-fs:
	docker exec etl-opensky-hdfs-namenode-1 hadoop fs -ls -C / | xargs /opt/hadoop/bin/hadoop fs -rm -R

run_dag:
	docker exec etl-opensky-airflow-1 airflow dags test flights_daily 2018-01-01T00:00:00+00:00

run_extract:
	docker exec etl-opensky-airflow-1 airflow tasks run flights_daily extract_flights scheduled__2018-01-01T00:00:00+00:00

run_load_dim_dates:
	docker exec etl-opensky-airflow-1 airflow tasks run flights_daily load_dim_dates scheduled__2018-01-01T00:00:00+00:00