FROM apache/airflow:slim-2.7.3

# Migrate DB & create account
RUN airflow db migrate \
    && airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@localhost

WORKDIR /opt/airflow

RUN mkdir -p logs temp \
    && chmod a+rw logs temp

# Install Airflow & Python packages
COPY ./requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Init webserver & scheduler
ENTRYPOINT ["airflow", "standalone"]