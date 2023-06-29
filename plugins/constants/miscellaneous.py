from enum import Enum

# Task type
MYSQL_TO_BQ    = 'mysql_to_bq'
POSTGRES_TO_BQ = 'postgres_to_bq'

RDBMS_TO_BQ = Enum('RDBMS_TO_BQ', [MYSQL_TO_BQ,
                                   POSTGRES_TO_BQ])

# BigQuery
WRITE_EMPTY    = 'WRITE_EMPTY'
WRITE_TRUNCATE = 'WRITE_TRUNCATE'
WRITE_APPEND   = 'WRITE_APPEND'

BQ_WRITE_DISPOSITION = Enum('BQ_WRITE_DISPOSITION', [WRITE_EMPTY,
                                                     WRITE_TRUNCATE,
                                                     WRITE_APPEND])

# Spark
SPARK_KUBERNETES_OPERATOR = 'spark_kubernetes_operator'
SPARK_KUBERNETES_SENSOR   = 'spark_kubernetes_sensor'

EXTENDED_SCHEMA = [
    {
        'mode': 'NULLABLE',
        'name': 'load_timestamp',
        'type': 'TIMESTAMP',
    }
]
