-include .env

.PHONY: provision-kind-cluster delete-kind-cluster build-airflow-image tag-airlfow-image push-airflow-image add-all-repos install-airflow install-mysql-db install-postgresql-db install-spark-on-k8s-operator create-spark-airflow-rb pf-airflow-webserver

provision-kind-cluster:
	@./k8s/provision.sh $(CLUSTER_NAME)

delete-kind-cluster:
	@kind delete cluster --name $(CLUSTER_NAME)

build-airflow-image:
	docker build -t airflow:2.9.3-python3.11 -f airflow.Dockerfile .

tag-airlfow-image:
	docker tag airflow:2.9.3-python3.11 localhost:5001/airflow:2.9.3-python3.11

push-airflow-image:
	docker push localhost:5001/airflow:2.9.3-python3.11

add-airflow-repo:
	helm repo add apache-airflow https://airflow.apache.org && \
	helm repo update

install-airflow:
	@helm install airflow -f helm/values/airflow.yaml apache-airflow/airflow \
	--set fernetKey=$(FERNET_KEY) \
	--set webserverSecretKey=$(WEBSERVER_SECRET_KEY) \
	--set dags.gitSync.repo=$(REPO) \
	--set config.logging.remote_base_log_folder=$(REMOTE_BASE_LOG_FOLDER) \
	--namespace airflow --debug --wait=false --timeout 20m

# PF stands for port-forward
pf-airflow-webserver:
	kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow

add-bitnami-repo:
	helm repo add bitnami https://charts.bitnami.com/bitnami && \
	helm repo update

install-mysql-db:
	helm install mysql-db -f helm/values/mysql.yaml bitnami/mysql --namespace mysql --create-namespace

install-postgresql-db:
	helm install postgresql-db -f helm/values/postgresql.yaml bitnami/postgresql --namespace postgresql --create-namespace

add-spark-operator-repo:
	helm repo add spark-operator https://kubeflow.github.io/spark-operator && \
	helm repo update

install-spark-on-k8s-operator:
	helm install spark -f helm/values/spark-on-k8s-operator.yaml spark-operator/spark-operator --namespace spark --create-namespace --set sparkJobNamespace=spark --set webhook.enable=true

# Create role and role binding from airflow:airflow-worker serviceaccount to spark resources
create-spark-airflow-rb:
	kubectl create role spark-app-airflow-role --verb=get,list,watch,create --resource=sparkapplications -n spark && \
	kubectl create rolebinding spark-app-airflow-role-bind --role=spark-app-airflow-role --serviceaccount=airflow:airflow-worker -n spark && \
	kubectl create role spark-pod-airflow-role --verb=get,list,watch --resource=pods,pods/log,pods/status -n spark && \
	kubectl create rolebinding spark-pod-airflow-role-bind --role=spark-pod-airflow-role --serviceaccount=airflow:airflow-worker -n spark

add-all-repos:
	helm repo add apache-airflow https://airflow.apache.org && \
	helm repo add bitnami https://charts.bitnami.com/bitnami && \
	helm repo add spark-operator https://kubeflow.github.io/spark-operator && \
	helm repo update
