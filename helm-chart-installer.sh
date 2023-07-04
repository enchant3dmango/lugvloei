# Add and update Helm repository
helm repo add apache-airflow https://airflow.apache.org
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

helm repo update

# Install Airflow
helm install airflow -f values-airflow.yaml apache-airflow/airflow --namespace airflow --debug --wait=false --timeout 20m

# Install MySQL
helm install mysql-db -f values-mysql.yaml bitnami/mysql --namespace mysql --create-namespace

# Install Spark-Operator and its prerequisites
kubectl create ns spark-job
helm install spark-operator spark-operator/spark-operator --namespace spark-operator --set sparkJobNamespace=spark-job --set webhook.enable=true --create-namespace
kubectl create serviceaccount spark -n spark
kubectl create clusterrolebinding spark-role-binding --clusterrole=edit --serviceaccount=spark-job:spark -n spark-job