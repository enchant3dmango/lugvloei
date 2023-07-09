from airflow.models import Variable

SPARK_JOB_NAMESPACE = Variable.get('SPARK_JOB_NAMESPACE', None)
RDBMS_TO_BQ_APPLICATION_FILE = Variable.get(
    'RDBMS_TO_BQ_APPLICATION_FILE', None)
