from enum import Enum
import os

# Task types
MYSQL_TO_BQ = "mysql_to_bq"
POSTGRES_TO_BQ = "postgres_to_bq"
RDBMS_TO_BQ = Enum("RDBMS_TO_BQ", [MYSQL_TO_BQ, POSTGRES_TO_BQ])

BQ_TO_PARQUET = "bq_to_parquet"

GSHEET_TO_BQ = "gsheet_to_bq"
GSHEET_TO_BQ_V2 = "gsheet_to_bq_v2"

# BigQuery load methods
DELSERT = "delsert"
UPSERT = "upsert"
TRUNCATE = "truncate"
APPEND = "append"
MERGE = Enum("MERGE", [DELSERT, UPSERT])

# Environment and miscellaneous variables
AIRFLOW = "airflow"
AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
DATABASE = "database"
DE_DAG_OWNER_NAME = "example@email.com"
PYTHONPATH = os.environ["PYTHONPATH"]
SPARK = "spark"
SPARK_KUBERNETES_OPERATOR = "o"
SPARK_KUBERNETES_SENSOR = "s"

# Extende schemas variables
EXTENDED_SCHEMA = [
    {
        "mode": "NULLABLE",
        "name": "load_timestamp",
        "type": "TIMESTAMP",
    }
]

EXTENDED_SCHEMA_OMS = [
    {
        "mode": "NULLABLE",
        "name": "database",
        "type": "STRING",
    },
    {
        "mode": "NULLABLE",
        "name": "host",
        "type": "STRING",
    },
    {
        "mode": "NULLABLE",
        "name": "load_timestamp",
        "type": "TIMESTAMP",
    },
]