#!/bin/sh
set -o errexit

# 1. Add and update helm repository
helm repo add apache-airflow https://airflow.apache.org
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo add datahub https://helm.datahubproject.io/
helm repo update

# 2. Install Airflow
helm install airflow -f charts/values-airflow.yaml apache-airflow/airflow --namespace airflow --debug --wait=false --timeout 20m

# 3. Install MySQL
helm install mysql-db -f charts/values-mysql.yaml bitnami/mysql --namespace mysql --create-namespace

# 4. Install PostgreSQL
helm install postgresql-db -f charts/values-postgresql.yaml bitnami/postgresql --namespace postgresql --create-namespace

# 5. Install Spark-Operator and its prerequisites
helm install spark -f charts/values-spark-on-k8s-operator.yaml spark-operator/spark-operator --namespace spark --create-namespace --set sparkJobNamespace=spark --set webhook.enable=true
# Create role and role binding from airflow:airflow-worker serviceaccount to spark resources
kubectl create role spark-app-airflow-role --verb=get,list,watch,create --resource=sparkapplications -n spark
kubectl create rolebinding spark-app-airflow-role-bind --role=spark-app-airflow-role --serviceaccount=airflow:airflow-worker -n spark
kubectl create role spark-pod-airflow-role --verb=get,list,watch --resource=pods,pods/log,pods/status -n spark
kubectl create rolebinding spark-pod-airflow-role-bind --role=spark-pod-airflow-role --serviceaccount=airflow:airflow-worker -n spark

# 6. Install Datahub and its prerequisites
kubectl create ns datahub
kubectl create secret generic -n datahub mysql-secrets --from-literal=mysql-root-password=root
helm install prerequisites datahub/datahub-prerequisites --values charts/values-datahub.yaml --namespace datahub --debug --wait=false --timeout 20m
helm install datahub datahub/datahub --namespace datahub --debug --wait=false --timeout 20m
