setup: download_data.sh
	bash download_data.sh \
		&& mkdir -p airflow/logs \
		&& chmod a+rw airflow/logs

up:
	docker compose up -d --build
	docker exec etl-opensky-spark-master-1 pip install --no-cache-dir -r /requirements.txt
	docker exec etl-opensky-spark-worker-1 pip install --no-cache-dir -r /requirements.txt
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop

purge-fs:
	docker exec etl-opensky-hdfs-namenode-1 hadoop fs -ls -C / | xargs /opt/hadoop/bin/hadoop fs -rm -R