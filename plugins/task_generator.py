import time
from plugins.constants.miscellaneous import (MYSQL_TO_BQ, POSTGRES_TO_BQ,
                                             RDBMS_TO_BQ)
from plugins.task_generator_set.rdbms_to_bq.rdbms_to_bq import RdbmsToBq


def generate_task(dag_id, config):

    rdbms_to_bq = RdbmsToBq(dag_id=dag_id, config=config)

    if MYSQL_TO_BQ in RDBMS_TO_BQ.__members__ or POSTGRES_TO_BQ in RDBMS_TO_BQ.__members__:
        rdbms_to_bq.generate_task()
        time.sleep(30)
    # TODO: Add conditional statement for other task type here 
