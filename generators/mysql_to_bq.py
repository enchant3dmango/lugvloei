from datetime import timedelta

from airflow.decorators import dag, task

class MySQLToBQ:
    """
        Class for MySQL to BQ DAG generator
    """
    
    def __init__(self, config, dag_id) -> None:
        self.config : list  = config
        self.dag_id : str   = dag_id
        
        default_args = {
            'owner'              : config.get('dag')['owner'],
            'depend_on_past'     : config.get('dag')['depends_on_past'],
            'email'              : config.get('dag')['email'],
            'email_on_failure'   : config.get('dag')['email_on_failure'],
            'email_on_retry'     : config.get('dag')['email_on_retry'],
            'schedule_interval'  : config.get('dag')['schedule_interval'],
            'concurrency'        : config.get('dag')['concurrency'],
            'retries'            : config.get('dag')['retry']['count'],
            'retry_delay'        : timedelta(minutes=config.get('dag')['retry']['delay']),
        }
    
        @dag(dag_id=self.dag_id, start_date=self.config.get('dag')['start_date'], default_args=default_args)
        def dynamic_generated_dag():
            @task
            def print_message(message):
                print(message)

            print_message(config)

        dynamic_generated_dag()