from airflow.models import Variable

SPARK_JOB_NAMESPACE = Variable.get('SPARK_JOB_NAMESPACE', None)
GCP_CONN_ID = Variable.get('GCP_CONN_ID', None)
GCS_DATA_LAKE_BUCKET = Variable.get('GCS_DATA_LAKE_BUCKET', None)
