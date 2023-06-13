FROM apache/airflow:2.5.0-python3.8

USER root

RUN  apt-get update -y && apt-get install -y \
    procps \
    vim
COPY dags $AIRFLOW_HOME/dags
COPY requirements.txt requirements.txt

USER airflow
RUN pip install --upgrade --ignore-installed pip
RUN pip install --no-cache-dir -r requirements.txt
