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
kubectl create ns spark
helm install spark-operator spark-operator/spark-operator --namespace spark-operator --set sparkJobNamespace=spark --set webhook.enable=true --create-namespace
kubectl create serviceaccount spark-job -n spark
kubectl create clusterrolebinding spark-crb --clusterrole=edit --serviceaccount=spark:spark-job -n spark

# Create role binding
kubectl create serviceaccount airflow-spark -n airflow
kubectl create role airflow-spark-role --verb=get,list,watch,create --resource=sparkapplications -n spark
kubectl create rolebinding airflow-spark-rb --role=airflow-spark-role --serviceaccount=airflow:airflow-spark -n spark