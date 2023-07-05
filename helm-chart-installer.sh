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
helm install spark -f values-spark-on-k8s-operator.yaml spark-operator/spark-operator --namespace spark --create-namespace --set sparkJobNamespace=spark --set webhook.enable=true

# Create role binding from Airflow worker service account to spark
kubectl create role spark-airflow-role --verb=get,list,watch,create --resource=sparkapplications -n spark
kubectl create rolebinding spark-airflow-role-bind --role=spark-airflow-role --serviceaccount=airflow:airflow-worker -n spark