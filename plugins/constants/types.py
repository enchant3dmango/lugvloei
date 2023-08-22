from enum import Enum
import os

# Task type
MYSQL_TO_BQ = 'mysql_to_bq'
POSTGRES_TO_BQ = 'postgres_to_bq'

RDBMS_TO_BQ = Enum('RDBMS_TO_BQ', [MYSQL_TO_BQ,
                                   POSTGRES_TO_BQ])

# BigQuery
DELSERT = 'delsert'
UPSERT = 'upsert'
TRUNCATE = 'truncate'

BQ_LOAD_METHOD = Enum('BQ_LOAD_METHOD', [DELSERT,
                                         UPSERT,
                                         TRUNCATE])

# Spark
SPARK_KUBERNETES_OPERATOR = 'operator'
SPARK_KUBERNETES_SENSOR = 'sensor'

EXTENDED_SCHEMA = [
    {
        'mode': 'NULLABLE',
        'name': 'load_timestamp',
        'type': 'TIMESTAMP',
    }
]

# Environment
PYTHONPATH = os.environ["PYTHONPATH"]
AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

# Ownership
DAG_OWNER_NAME = 'example@email.com'
