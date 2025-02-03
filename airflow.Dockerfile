FROM apache/airflow:2.9.3-python3.11

# Download and install Rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

COPY airflow.requirements.txt /tmp/requirements.txt

# Upgrade pip and install dependencies in requirements.txt
RUN pip install --upgrade --ignore-installed pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt