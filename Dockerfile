FROM apache/airflow:2.5.0-python3.8

# Download and install Rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

COPY ./requirements.txt /tmp/requirements.txt

# Upgrade pip and install dependencies in requirements.txt
RUN pip install --upgrade --ignore-installed pip & \
    pip install --no-cache-dir -r /tmp/requirements.txt
