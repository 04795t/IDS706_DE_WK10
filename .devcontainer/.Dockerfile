FROM apache/airflow:3.1.0

USER root

RUN apt-get update && \
    apt-get install -y default-jre procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt

USER root
RUN mkdir -p /opt/airflow/data
COPY data/ /opt/airflow/data/
RUN chown -R airflow:root /opt/airflow/data

USER airflow