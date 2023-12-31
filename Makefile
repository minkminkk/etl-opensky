setup: download_data.sh
	bash download_data.sh \
		&& mkdir -p containers/airflow/logs \
		&& chmod a+rw containers/airflow/logs
	python src/setup_jobs.py install

up:
	docker compose up -d --build
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop

purge-fs:
	docker exec etl-opensky-hdfs-namenode-1 hadoop fs -ls -C / | xargs /opt/hadoop/bin/hadoop fs -rm -R