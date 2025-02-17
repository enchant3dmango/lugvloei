-include .env

.PHONY: install uninstall \
	airflow-build airflow-install \
	airflow-uninstall airflow-webserver-pf \
	pg-install pg-uninstall pg-pf \
	spark-install spark-airflow-rb

install:
	@./k8s/provision.sh $(CLUSTER_NAME)

uninstall:
	@kind delete cluster --name $(CLUSTER_NAME)

airflow-build:
	@docker build -t airflow:2.9.3-python3.11 -f airflow.Dockerfile . && \
	docker tag airflow:2.9.3-python3.11 localhost:5001/airflow:2.9.3-python3.11 && \
	docker push localhost:5001/airflow:2.9.3-python3.11

airflow-install:
	@helm repo add apache-airflow https://airflow.apache.org && \
	helm repo update && \
	helm install airflow -f helm/values/airflow.yaml apache-airflow/airflow \
	--set dags.gitSync.repo=$(AIRFLOW_DAGS_GIT_SYNC_REPO) \
	--set fernetKey=$(AIRFLOW_FERNET_KEY) \
	--set config.logging.remote_base_log_folder=$(AIRFLOW_REMOTE_BASE_LOG_FOLDER) \
	--set webserverSecretKey=$(AIRFLOW_WEBSERVER_SECRET_KEY) \
	--namespace airflow --debug --wait=false --timeout 20m

airflow-uninstall:
	@helm uninstall airflow --namespace airflow

# Port-forward (pf) the Airflow Webserver so you can open it in local browser
airflow-webserver-pf:
	kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow

pg-install:
	@helm repo add bitnami https://charts.bitnami.com/bitnami && \
	helm repo update && \
	helm install postgresql-db -f helm/values/postgresql.yaml bitnami/postgresql \
	--set auth.database=$(POSTGRESQL_AUTH_DATABASE) \
	--set auth.username=$(POSTGRESQL_AUTH_USERNAME) \
	--set auth.password=$(POSTGRESQL_AUTH_PASSWORD) \
	--namespace postgresql --create-namespace

pg-uninstall:
	@helm uninstall postgresql-db --namespace postgresql

pg-pf:
	kubectl port-forward svc/postgresql-db 5432:5432 --namespace postgresql

spark-install:
	@helm repo add spark-operator https://kubeflow.github.io/spark-operator && \
	helm repo update && \
	helm install spark -f helm/values/spark-on-k8s-operator.yaml spark-operator/spark-operator \
	--set sparkJobNamespace=spark --set webhook.enable=true \
	--namespace spark --create-namespace

# Create role and role binding from airflow:airflow-worker serviceaccount to spark resources
spark-airflow-rb:
	@kubectl create role spark-app-airflow-role --verb=get,list,watch,create --resource=sparkapplications -n spark && \
	kubectl create rolebinding spark-app-airflow-role-bind --role=spark-app-airflow-role --serviceaccount=airflow:airflow-worker -n spark && \
	kubectl create role spark-pod-airflow-role --verb=get,list,watch --resource=pods,pods/log,pods/status -n spark && \
	kubectl create rolebinding spark-pod-airflow-role-bind --role=spark-pod-airflow-role --serviceaccount=airflow:airflow-worker -n spark
