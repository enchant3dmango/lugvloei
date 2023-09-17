FROM apache/airflow:2.5.0-python3.8

COPY ./requirements.txt /tmp/requirements.txt

RUN pip install --upgrade --ignore-installed pip & \
    pip install --no-cache-dir -r /tmp/requirements.txt