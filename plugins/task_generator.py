from plugins.constants.types import RDBMS_TO_BQ
from plugins.task_generators.rdbms_to_bq.rdbms_to_bq import RDBMSToBQGenerator 


def generate_task(dag_id, config):

    rdbms_to_bq = RDBMSToBQGenerator(dag_id=dag_id, config=config)

    if config['type'] in RDBMS_TO_BQ.__members__:
        rdbms_to_bq.generate_task()
    # TODO: Add conditional statement for other task type here
