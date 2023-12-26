setup: download_data.sh
	bash download_data.sh \
		&& mkdir -p airflow/logs \
		&& chmod a+rw airflow/logs
up:
	docker compose up -d
# docker exec etl-opensky-spark-master-1 pip install --no-cache-dir -r /requirements.txt
# docker exec etl-opensky-spark-worker-1 pip install --no-cache-dir -r /requirements.txt
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop
spark-submit:
	docker exec etl-opensky-spark-master-1 spark-submit /jobs/extract.py EDDF "2018-01-28 00:00:00" "2018-01-29 00:00:00"