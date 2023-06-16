from enum import Enum

MYSQL_TO_BQ = 'mysql_to_bq'
POSTGRES_TO_BQ = 'postgres_to_bq'

BRONZE = 'bronze'
SILVER = 'silver'

RDBMS_TO_BQ = Enum('RDBMS_TO_BQ', [MYSQL_TO_BQ, 
                                   POSTGRES_TO_BQ])
