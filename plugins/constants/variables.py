from airflow.models import Variable

SPARK_JOB_NAMESPACE = Variable.get('SPARK_JOB_NAMESPACE', None)
RDBMS_TO_BQ_APPLICATION_FILE = Variable.get(
    'RDBMS_TO_BQ_APPLICATION_FILE', None)
BQ_TO_PARQUET_APPLICATION_FILE = Variable.get(
    'BQ_TO_PARQUET_APPLICATION_FILE', None)

if int(Variable.get('DAG_GENERATOR_FEATURE_FLAG', 0)) in [0, 1]:
    DAG_GENERATOR_FEATURE_FLAG = False if int(
        Variable.get('DAG_GENERATOR_FEATURE_FLAG', 0)) == 0 else True
else:
    raise ValueError("DAG_GENERATOR_FEATURE_FLAG must be 0 or 1.")

GCP_CONN_ID = Variable.get('GCP_CONN_ID', None)

GCS_DATA_LAKE_BUCKET = Variable.get('GCS_DATA_LAKE_BUCKET', None)
