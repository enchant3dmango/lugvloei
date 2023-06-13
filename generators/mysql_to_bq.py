from airflow.decorators import task


@task
def task_1(dag_id):
    print(f"Hello from task_1 with dag_id {dag_id}")


@task
def task_2(dag_id):
    print(f"Hello from task_2 with dag_id {dag_id}")


@task
def task_3(config):
    print(f"Hello from task_1 with config {config}")


def generate_task_flow(config, dag_id):
    t_1 = task_1(dag_id)
    t_2 = task_2(dag_id)
    t_3 = task_3(config)

    t_1 >> t_2
    t_2 >> t_3

    return [t_1, t_2, t_3]
