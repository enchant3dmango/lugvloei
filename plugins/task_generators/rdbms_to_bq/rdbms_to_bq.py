import json
import logging
import os
from ast import literal_eval
from typing import List, Union
from urllib.parse import urlencode

import yaml
from google.cloud import bigquery
from google.cloud.bigquery import (DestinationFormat, SourceFormat,
                                   WriteDisposition)

from airflow.hooks.base import BaseHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator, BigQueryInsertJobOperator)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from plugins.constants.types import (AIRFLOW, APPEND, DATABASE, DELSERT,
                                     EXTENDED_SCHEMA, MERGE, MYSQL_TO_BQ,
                                     POSTGRES_TO_BQ, PYTHONPATH, SPARK,
                                     SPARK_KUBERNETES_OPERATOR,
                                     SPARK_KUBERNETES_SENSOR, UPSERT)
from plugins.constants.variables import (DEFAULT_GCS_BUCKET, GCP_CONN_ID,
                                         RDBMS_TO_BQ_APPLICATION_FILE,
                                         SPARK_JOB_NAMESPACE)
from plugins.task_generators.rdbms_to_bq.types import (
    DELSERT_QUERY, SOURCE_EXTRACT_QUERY, TEMP_TABLE_PARTITION_DATE_QUERY,
    UPSERT_QUERY)
from plugins.utilities.generic import get_iso8601_date, get_onelined_string


class RDBMSToBQGenerator:
    """
    Constructor arguments:
        dag_id (Required[str]):
            The ID of the Directed Acyclic Graph (DAG) to which the generated tasks belong.
        config (Required[dict]):
            A dictionary containing configuration parameters for the data transfer process.
        **kwargs:
            Additional keyword arguments.
    """

    def __init__(self, dag_id: str, config: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.dag_id                    : str                   = dag_id
        self.bq_client                 : bigquery.Client       = bigquery.Client()
        self.task_type                 : str                   = config['type']
        self.task_mode                 : str                   = config['mode']
        self.source_connection         : Union[str, List[str]] = config['source']['connection']
        self.source_schema             : str                   = config['source']['schema']
        self.source_table              : str                   = config['source']['table']
        self.source_timestamp_keys     : List[str]             = config['source']['timestamp_keys']
        self.source_unique_keys        : List[str]             = config['source']['unique_keys']
        self.target_bq_project         : str                   = config['target']['bq']['project']
        self.target_bq_dataset         : str                   = config['target']['bq']['dataset']
        self.target_bq_table           : str                   = config['target']['bq']['table']
        self.target_bq_table_temp      : str                   = f'{self.target_bq_table}_temp'
        self.target_bq_load_method     : str                   = config['target']['bq']['load_method']
        self.target_bq_partition_key   : str                   = config['target']['bq']['partition_key']
        self.full_target_bq_table      : str                   = f'{self.target_bq_project}.{self.target_bq_dataset}.{self.target_bq_table}'
        self.full_target_bq_table_temp : str                   = f'{self.target_bq_project}.{self.target_bq_dataset}.{self.target_bq_table_temp}'

        if self.task_type == POSTGRES_TO_BQ:
            self.quoting = lambda text: f'"{text}"'
        elif self.task_type == MYSQL_TO_BQ:
            self.quoting = lambda text: f'`{text}`'
        else:
            raise Exception('Task type is not supported!')

        self.dag_base_path = f'{os.environ["PYTHONPATH"]}/dags/{self.target_bq_dataset}/{self.target_bq_table}'

    def __generate_schema(self, **kwargs) -> list:
        schema_file = os.path.join(self.dag_base_path, "assets/schema.json")

        try:
            logging.info(f'Getting table schema from {schema_file}')
            with open(schema_file, "r") as file:
                schema = json.load(file)
                file.close()

            fields = [schema_detail["name"] for schema_detail in schema]

            # Extend schema from EXTENDED_SCHEMA
            schema.extend(
                [
                    schema_detail
                    for schema_detail in EXTENDED_SCHEMA
                    if schema_detail["name"] not in fields
                ]
            )
        except:
            raise Exception('Failed to generate schema!')

        return schema

    def __generate_extract_query(self, schema: list = None, **kwargs) -> str:
        # Get all field name and exclude the extended field name to be selected
        if schema is not None:
            extended_fields = [schema_detail["name"]
                               for schema_detail in EXTENDED_SCHEMA]
            fields = [
                # Cast all column with string type into text
                f"{self.quoting(schema_detail['name'])}::text"
                if schema_detail["type"] == 'STRING'
                else self.quoting(schema_detail['name'])
                for schema_detail in schema
                if schema_detail["name"] not in extended_fields
            ]

            selected_fields = ', '.join([
                field for field in fields
                if field != DATABASE  # Exclude database field
            ])

        # Generate query
        source_extract_query = SOURCE_EXTRACT_QUERY.substitute(
            selected_fields=selected_fields,
            load_timestamp='CURRENT_TIMESTAMP' if self.task_type == POSTGRES_TO_BQ else 'CURRENT_TIMESTAMP()',
            source_table_name=self.source_table if self.source_schema is None else f'{self.source_schema}.{self.source_table}',
        )

        # Add custom value for database field based on connection name
        # This is intended for multiple connection dag
        if kwargs.get(DATABASE) is not None:
            database = str(kwargs[DATABASE]).replace(
                'pg_', '').replace('mysql_', '')
            source_extract_query = source_extract_query.replace(
                " FROM",
                f", '{database}' AS {self.quoting('database')} FROM"
            )

        # Generate query filter based on target_bq_load_method
        if self.target_bq_load_method in MERGE.__members__ or self.target_bq_load_method == APPEND:
            # Create the condition for filtering based on timestamp_keys
            condition = ' OR '.join(
                [
                    f"{self.quoting(timestamp_key)} >=  '{{{{ data_interval_start.astimezone(dag.timezone) }}}}' AND {self.quoting(timestamp_key)} < '{{{{ data_interval_end.astimezone(dag.timezone) }}}}'"
                    for timestamp_key in self.source_timestamp_keys
                ]
            )

            # Append extract query condition
            source_extract_query = source_extract_query + f" WHERE {condition}"

        logging.info(f'Extract query: {source_extract_query}')

        if self.task_mode == SPARK:
            source_extract_query = f'({source_extract_query}) AS cte'

        return get_onelined_string(source_extract_query)

    def __generate_merge_query(self, schema, **kwargs) -> str:
        partition_filter = ''
        query = None

        # Query to get partition_key date list from temp table to be used as partition filter
        if self.target_bq_partition_key is not None:
            temp_table_partition_date_query = TEMP_TABLE_PARTITION_DATE_QUERY.substitute(
                partition_key=self.target_bq_partition_key,
                target_bq_table_temp=self.full_target_bq_table_temp
            )
            partition_filter = f"AND DATE(x.{self.target_bq_partition_key}) IN UNNEST(formatted_dates)"

        # Use cte to get the latest source table data for the merge statement
        # Only applied to dag that has cte_merge.sql in its assets folder
        # Will use the temporary table as merge source if no cte_merge.sql is provided
        merge_cte = os.path.join(self.dag_base_path, "assets/cte_merge.sql")
        if os.path.exists(merge_cte):
            with open(merge_cte, "r") as file:
                merge_source = file.read()
                merge_source = merge_source.format(
                    full_target_bq_table_temp=self.full_target_bq_table_temp
                )
        else:
            merge_source = f"`{self.full_target_bq_table_temp}`"

        # Construct delsert query
        if self.target_bq_load_method == DELSERT:
            merge_query = DELSERT_QUERY.substitute(
                merge_target=self.full_target_bq_table,
                merge_source=merge_source,
                on_keys=' AND '.join(
                    [f"COALESCE(CAST(x.`{key}` as string), 'NULL') = COALESCE(CAST(y.`{key}` as string), 'NULL')" for key in self.source_unique_keys]),
                partition_filter=partition_filter,
                insert_fields=', '.join(
                    [f"`{field['name']}`" for field in schema])
            )

            query = temp_table_partition_date_query + merge_query
            logging.info(f'Delsert query: {query}')

        # Construct upsert query
        elif self.target_bq_load_method == UPSERT:
            merge_query = UPSERT_QUERY.substitute(
                merge_target=self.full_target_bq_table,
                merge_source=merge_source,
                on_keys=' AND '.join(
                    [f"COALESCE(CAST(x.`{key}` as string), 'NULL') = COALESCE(CAST(y.`{key}` as string), 'NULL')" for key in self.source_unique_keys]),
                partition_filter=partition_filter,
                update_fields=', '.join(
                    [f"x.`{field['name']}` = y.`{field['name']}`" for field in schema]),
                insert_fields=', '.join(
                    [f"`{field['name']}`" for field in schema])
            )

            query = temp_table_partition_date_query + merge_query
            logging.info(f'Upsert query: {query}')

        return get_onelined_string(f'{query}')

    def __get_conn(self, **kwargs) -> str:
        return BaseHook.get_connection(self.source_connection)

    def __generate_jdbc_url(self, **kwargs) -> str:
        jdbc_uri = f'jdbc:{self.__get_conn().get_uri()}'
        jdbc_uri.replace(
            'postgres', 'postgresql') if self.task_type == POSTGRES_TO_BQ else jdbc_uri

        db_type = jdbc_uri.split("://")[0]
        db_conn = jdbc_uri.split("@")[1].split("?")[0]

        return f'{db_type}://{db_conn}?{self.__generate_jdbc_urlencoded_extra()}' if self.__get_conn().extra else f'{db_type}://{db_conn}'

    def __generate_jdbc_credential(self, **kwargs) -> List[str]:
        credential = self.__get_conn()

        return f'{credential.login}:{credential.password}'

    def __generate_jdbc_urlencoded_extra(self, **kwargs):
        extras = self.__get_conn().extra

        return urlencode(literal_eval(extras))

    def generate_tasks(self):
        if self.task_mode == SPARK:
            # Task generator for single connection dag
            if type(self.source_connection) is str:
                schema_string = f'{json.dumps(self.__generate_schema(), separators=(",", ":"))}'
                onelined_schema_string = get_onelined_string(schema_string)

                schema = self.__generate_schema()
                extract_query = self.__generate_extract_query(schema=schema)
                merge_query = self.__generate_merge_query(schema=schema)

                with open(f'{PYTHONPATH}/{RDBMS_TO_BQ_APPLICATION_FILE}') as f:
                    application_file = yaml.safe_load(f)

                application_file['spec']['arguments'] = [
                    f"--target_bq_load_method={self.target_bq_load_method}",
                    f"--source_timestamp_keys={','.join(self.source_timestamp_keys)}",
                    f"--full_target_bq_table={self.full_target_bq_table}",
                    f"--target_bq_project={self.target_bq_project}",
                    f"--jdbc_credential={self.__generate_jdbc_credential()}",
                    f"--partition_key={self.target_bq_partition_key}",
                    f"--extract_query={extract_query}",
                    f"--merge_query={merge_query}",
                    f"--task_type={self.task_type}",
                    f"--jdbc_url={self.__generate_jdbc_url()}",
                    f"--schema={onelined_schema_string}",
                    # TODO: Later, send master url
                ]

                spark_kubernetes_base_task_id = f'{self.target_bq_dataset}-{self.target_bq_table}'.replace(
                    '_', '-').replace('bronze-', '')
                spark_kubernetes_operator_task_id = f'{spark_kubernetes_base_task_id}-{SPARK_KUBERNETES_OPERATOR}'
                spark_kubernetes_operator_task = SparkKubernetesOperator(
                    task_id          = spark_kubernetes_operator_task_id,
                    application_file = yaml.safe_dump(application_file),
                    namespace        = SPARK_JOB_NAMESPACE,
                    do_xcom_push     = True
                )

                spark_kubernetes_sensor_task = SparkKubernetesSensor(
                    task_id          = f"{spark_kubernetes_base_task_id}-{SPARK_KUBERNETES_SENSOR}",
                    namespace        = SPARK_JOB_NAMESPACE,
                    application_name = f"{{{{ task_instance.xcom_pull(task_ids='{spark_kubernetes_operator_task_id}')['metadata']['name'] }}}}",
                    attach_log       = True
                )

                return spark_kubernetes_operator_task >> spark_kubernetes_sensor_task

        elif self.task_mode == AIRFLOW:
            schema = self.__generate_schema()
            iso8601_date = get_iso8601_date()

            # Use WRITE_APPEND if the load method is APPEND, else, use WRITE_TRUNCATE
            write_disposition = WriteDisposition.WRITE_APPEND if self.target_bq_load_method == APPEND else WriteDisposition.WRITE_TRUNCATE

            time_partitioning = {
                "type": "DAY",
                "field": self.target_bq_partition_key
            } if self.target_bq_partition_key else None

            # Task generator for single connection dag
            if type(self.source_connection) is str:
                extract_query = self.__generate_extract_query(schema=schema)
                filename = f'{self.target_bq_dataset}/{self.target_bq_table}/{iso8601_date}/{self.source_table}' + '__{}.json'

                # Extract data from Postgres, then load to GCS
                if self.task_type == POSTGRES_TO_BQ:
                    extract = PostgresToGCSOperator(
                        task_id          = f'extract__{self.source_table}',
                        postgres_conn_id = self.source_connection,
                        gcp_conn_id      = GCP_CONN_ID,
                        sql              = extract_query,
                        bucket           = DEFAULT_GCS_BUCKET,
                        export_format    = DestinationFormat.NEWLINE_DELIMITED_JSON,
                        filename         = filename,
                        write_on_empty   = True,
                        schema           = schema,
                        stringify_dict   = True
                    )

                # Extract data from MySQL, then load to GCS
                elif self.task_type == MYSQL_TO_BQ:
                    extract = MySQLToGCSOperator(
                        task_id        = f'extract__{self.source_table}',
                        mysql_conn_id  = self.source_connection,
                        gcp_conn_id    = GCP_CONN_ID,
                        sql            = extract_query,
                        bucket         = DEFAULT_GCS_BUCKET,
                        export_format  = DestinationFormat.NEWLINE_DELIMITED_JSON,
                        filename       = filename,
                        write_on_empty = True,
                        schema         = schema,
                        stringify_dict   = True
                    )

            # Task generator for multiple connection dag
            elif type(self.source_connection) is list:
                extract = []

                for index, connection in enumerate(sorted(self.source_connection)):
                    extract_query = self.__generate_extract_query(schema=schema, database=connection)
                    filename = f'{self.target_bq_dataset}/{self.target_bq_table}/{iso8601_date}/{self.source_table}_{index+1}' + '__{}.json'

                    # Extract data from Postgres, then load to GCS
                    if self.task_type == POSTGRES_TO_BQ:
                        __extract = PostgresToGCSOperator(
                            task_id          = f'extract__{self.source_table}_{index+1}',
                            postgres_conn_id = connection,
                            gcp_conn_id      = GCP_CONN_ID,
                            sql              = extract_query,
                            bucket           = DEFAULT_GCS_BUCKET,
                            export_format    = DestinationFormat.NEWLINE_DELIMITED_JSON,
                            filename         = filename,
                            write_on_empty   = True,
                            schema           = schema,
                            stringify_dict   = True
                        )

                    # Extract data from MySQL, then load to GCS
                    elif self.task_type == MYSQL_TO_BQ:
                        __extract = MySQLToGCSOperator(
                            task_id        = f'extract__{self.source_table}_{index+1}',
                            mysql_conn_id  = connection,
                            gcp_conn_id    = GCP_CONN_ID,
                            sql            = extract_query,
                            bucket         = DEFAULT_GCS_BUCKET,
                            export_format  = DestinationFormat.NEWLINE_DELIMITED_JSON,
                            filename       = filename,
                            write_on_empty = True,
                            schema         = schema,
                            stringify_dict = True
                        )

                    extract.append(__extract)

            # Directly load the data into BigQuery main table if the load method is TRUNCATE or APPEND, else, load it to temporary table first
            destination_project_dataset_table = f'{self.full_target_bq_table}' if self.target_bq_load_method not in MERGE.__members__ \
                else f'{self.full_target_bq_table_temp}'

            # Load data from GCS to BigQuery
            load = GCSToBigQueryOperator(
                task_id                           = f'load_to_bq__{self.source_table}',
                gcp_conn_id                       = GCP_CONN_ID,
                bucket                            = DEFAULT_GCS_BUCKET,
                destination_project_dataset_table = destination_project_dataset_table,
                source_objects                    = [f'{self.target_bq_dataset}/{self.target_bq_table}/{iso8601_date}/*.json'],
                schema_fields                     = schema,
                source_format                     = SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition                 = write_disposition,
                time_partitioning                 = time_partitioning,
                autodetect                        = False
            )

            # Add extra task to merge the data from temporary table into main table if the load method is DELSERT or UPSERT
            if self.target_bq_load_method in MERGE.__members__:
                merge_query = self.__generate_merge_query(schema=schema)

                # JobConfiguration, https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
                configuration = {
                    "query": {
                        "query": merge_query,
                        "useLegacySql": False
                    }
                }

                # Merge temporary table into main table
                merge = BigQueryInsertJobOperator(
                    task_id       = f'merge__{self.source_table}',
                    project_id    = self.target_bq_project,
                    gcp_conn_id   = GCP_CONN_ID,
                    configuration = configuration
                )

                # Delete temporary table
                delete = BigQueryDeleteTableOperator(
                    task_id                = f'delete__{self.target_bq_table_temp}',
                    gcp_conn_id            = GCP_CONN_ID,
                    deletion_dataset_table = destination_project_dataset_table,
                    ignore_if_missing      = True
                )

                # Early return the task flow for MERGE load method
                return extract >> load >> merge >> delete

            # Task flow for TRUNCATE or APPEND method
            return extract >> load
