from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow.decorators import task


def generate_task_flow(config):

    spark_job_args = [
        # Note: Later, get the query from rdbms_to_bq_query_builder
        "--sql", f'{config}'
    ]

    spark_task = SparkSubmitOperator(
        task_id="spark_task",
        name=f'{config}',
        application="jars/{jar_name}.jar",
        application_args=spark_job_args,
        conn_id=config,  # Note: Adjust the config to get only conn_id
        verbose=False
    )

    return spark_task
