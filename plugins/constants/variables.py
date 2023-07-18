from airflow.models import Variable

SPARK_JOB_NAMESPACE = Variable.get('SPARK_JOB_NAMESPACE', None)
RDBMS_TO_BQ_APPLICATION_FILE = Variable.get(
    'RDBMS_TO_BQ_APPLICATION_FILE', None)

if int(Variable.get('DAG_GENERATOR_FEATURE_FLAG')) in [0, 1]:
    DAG_GENERATOR_FEATURE_FLAG = False if int(
        Variable.get('DAG_GENERATOR_FEATURE_FLAG')) == 0 else True
else:
    raise ValueError("DAG_GENERATOR_FEATURE_FLAG must be 0 or 1.")

GCP_CONN_ID = Variable.get('GCP_CONN_ID', None)
