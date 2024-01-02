setup: download_data.sh
	bash download_data.sh \
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