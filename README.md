
Install Docker (Tested on v27.4.0, build bde2b89) or Docker Desktop (Tested on v4.37.2)
Install kind (Tested on v0.26.0)
Install kubectl (Tested on v1.32.1)
Setup GCP project

Create a service account, has read and write access to GCS
Store the service account as serviceaccount.json in files/ directory

Setup Docker Hub account (login to your account via Docker CLI or Docker Desktop)

Fork repository
Update values-airflow.yaml (check TODO)

I hardcoded the fernetKey and webserverSecretKey (it's not good), but let's ignore it for now bcoz this is just for learning purpose

To make the library available in your local device
pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
pip install -r requirements.txt