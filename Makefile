setup: setup.sh
	bash setup.sh
run:
	python3 ./dags/extract.py EDDF "2018-01-28 00:00:00" "2018-02-10 00:00:00"