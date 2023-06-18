from enum import Enum

# Task type
MYSQL_TO_BQ = 'mysql_to_bq'
POSTGRES_TO_BQ = 'postgres_to_bq'

RDBMS_TO_BQ = Enum('RDBMS_TO_BQ', [MYSQL_TO_BQ,
                                   POSTGRES_TO_BQ])

# DAG level
BRONZE = 'bronze'
SILVER = 'silver'

# Template
SUBMIT_SPARK_JOB = 'submit_spark_job'