from airflow.models import Variable

SPARK_JOB_NAMESPACE = Variable.get('SPARK_JOB_NAMESPACE', None)
RDBMS_TO_BQ_APPLICATION_FILE = Variable.get(
    'RDBMS_TO_BQ_APPLICATION_FILE', None)
BQ_TO_PARQUET_APPLICATION_FILE = Variable.get(
    'BQ_TO_PARQUET_APPLICATION_FILE', None)

if int(Variable.get('DAG_GENERATOR_FEATURE_FLAG')) in [0, 1]:
    DAG_GENERATOR_FEATURE_FLAG = False if int(
        Variable.get('DAG_GENERATOR_FEATURE_FLAG')) == 0 else True
else:
    raise ValueError("DAG_GENERATOR_FEATURE_FLAG must be 0 or 1.")

GCP_CONN_ID = Variable.get('GCP_CONN_ID', None)

# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove those files that are 30 days old or older.
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = int(
    Variable.get("airflow_db_cleanup__max_db_entry_age_in_days", 30)
)
