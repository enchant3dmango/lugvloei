from airflow.models import Variable

SPARK_JOB_NAMESPACE = Variable.get('SPARK_JOB_NAMESPACE', None)
RDBMS_TO_BQ_APPLICATION_FILE = Variable.get(
    'RDBMS_TO_BQ_APPLICATION_FILE', None)

DAG_GENERATOR_FEATURE_FLAG = Variable.get('DAG_GENERATOR_FEATURE_FLAG', False)

GCP_CONN_ID = Variable.get('GCP_CONN_ID', None)
