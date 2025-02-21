from airflow.decorators import dag, task
import pendulum

start_date = pendulum.datetime(2025, 1, 25, tz="Asia/Jakarta")


default_args = {
    "priority_weight": 1,
    "depend_on_past": False,
    "retries": 0,
}


@dag(
    catchup=False,
    dag_id="backfiller",
    default_args=default_args,
    start_date=start_date,
    schedule=None,
)
def generate_dag():
    @task(
        executor_config={
            "KubernetesExecutor": {
                "request_cpu": "1000m",
                "limit_cpu": "1000m",
                "request_memory": "1024Mi",
                "limit_memory": "1024Mi",
                "node_selector": {
                    "eks.amazonaws.com/nodegroup": "other node group name"
                },
            }
        },
    )

    def test(**kwargs):
        print(kwargs)

    test
