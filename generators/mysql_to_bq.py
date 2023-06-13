from airflow.decorators import task


def dynamic_generated_dag(config, dag_id):
    @task
    def print_message(message):
        print(message)

    print_message(f'The config is {config}')
    print_message(f'The dag_id is {dag_id}')
