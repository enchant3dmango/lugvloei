import yaml
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import \
    SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import \
    SparkKubernetesSensor
from google.cloud import bigquery

from plugins.constants.types import (PYTHONPATH, SPARK_KUBERNETES_OPERATOR,
                                     SPARK_KUBERNETES_SENSOR)
from plugins.constants.variables import (BQ_TO_PARQUET_APPLICATION_FILE,
                                         SPARK_JOB_NAMESPACE)
from plugins.task_generators.bq_to_parquet.types import BQ_TABLE_EXTRACT_QUERY


class BQToParquetGenerator():
    def __init__(self, dag_id: str, config: dict, **kwargs) -> None:
        self.dag_id                 : str             = dag_id
        self.bq_client              : bigquery.Client = bigquery.Client()
        self.task_type              : str             = config['type']
        self.bq_project             : str             = config['source']['bq']['project']
        self.bq_dataset             : str             = config['source']['bq']['dataset']
        self.bq_table               : str             = config['source']['bq']['table']
        self.bq_fields              : str             = config['source']['bq']['fields']
        self.bq_v_dataset           : str             = config['source']['bq']['v_dataset']
        self.gcs_project            : str             = config['target']['gcs']['project']
        self.gcs_bucket             : str             = config['target']['gcs']['bucket']
        self.gcs_hive_partition_key : str             = config['target']['gcs']['hive_partition_key']


    def __generate_extract_query(self) -> str:
        return BQ_TABLE_EXTRACT_QUERY.substitute(
            fields={self.bq_fields} if self.bq_fields else '*',
            bq_table_name=f'{self.bq_project}.{self.bq_dataset}.{self.bq_table}'
        )

    def generate_tasks(self):
        with open(f'{PYTHONPATH}/{BQ_TO_PARQUET_APPLICATION_FILE}') as f:
            application_file = yaml.safe_load(f)

        application_file['spec']['arguments'] = [
            f"--extract_query={self.__generate_extract_query()}",
            f"--bq_project={self.bq_project}",
            f"--bq_dataset={self.bq_v_dataset}",
            f"--gcs_project={self.gcs_project}",
            f"--gcs_bucket={self.gcs_bucket}",
            f"--gcs_folder={self.bq_dataset}.{self.bq_table}",
            f"--hive_partition_key={self.gcs_hive_partition_key}"
        ]

        spark_kubernetes_base_task_id = f'{self.bq_v_dataset}-{self.bq_table}'.replace('_', '-')
        spark_kubernetes_operator_task_id = f'{spark_kubernetes_base_task_id}-{SPARK_KUBERNETES_OPERATOR}'
        spark_kubernetes_operator_task = SparkKubernetesOperator(
            task_id          = spark_kubernetes_operator_task_id,
            application_file = yaml.safe_dump(application_file),
            namespace        = SPARK_JOB_NAMESPACE,
            do_xcom_push     = True,
        )

        spark_kubernetes_sensor_task = SparkKubernetesSensor(
            task_id          = f"{spark_kubernetes_base_task_id}-{SPARK_KUBERNETES_SENSOR}",
            namespace        = SPARK_JOB_NAMESPACE,
            application_name = f"{{{{ task_instance.xcom_pull(task_ids='{spark_kubernetes_operator_task_id}')['metadata']['name'] }}}}",
            attach_log       = True
        )

        return spark_kubernetes_operator_task >> spark_kubernetes_sensor_task
