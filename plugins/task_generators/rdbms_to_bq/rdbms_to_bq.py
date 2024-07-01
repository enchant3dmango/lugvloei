import json
import logging
import os
from typing import List, Union

import yaml
from google.cloud.bigquery import (DestinationFormat, SourceFormat,
                                   WriteDisposition)

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator, BigQueryInsertJobOperator)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from plugins.constants.types import (APPEND, DATABASE, DELSERT,
                                     EXTENDED_SCHEMA, MERGE, MYSQL_TO_BQ,
                                     POSTGRES_TO_BQ, PYTHONPATH, UPSERT)
from plugins.constants.variables import (GCP_CONN_ID, GCS_DATA_LAKE_BUCKET)
from plugins.task_generators.rdbms_to_bq.types import (
    DELSERT_QUERY, SOURCE_EXTRACT_QUERY, TEMP_TABLE_PARTITION_DATE_QUERY,
    UPSERT_QUERY)
from plugins.utilities.general import get_onelined_string


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
        self.dag_id                              : str                   = dag_id
        self.task_type                           : str                   = config['type']
        self.source_connection                   : Union[str, List[str]] = config['source']['connection']
        self.source_schema                       : str                   = config['source']['schema']
        self.source_table                        : str                   = config['source']['table']
        self.source_start_window_expansion_value : int                   = config['source']['start_window_expansion']['value']
        self.source_start_window_expansion_unit  : str                   = config['source']['start_window_expansion']['unit']
        self.source_timestamp_keys               : List[str]             = config['source']['timestamp_keys']
        self.source_unique_keys                  : List[str]             = config['source']['unique_keys']
        self.target_bq_project                   : str                   = config['target']['bq']['project']
        self.target_bq_dataset                   : str                   = config['target']['bq']['dataset']
        self.target_bq_table                     : str                   = config['target']['bq']['table']
        self.target_bq_load_method               : str                   = config['target']['bq']['load_method']
        self.target_bq_partition_field           : str                   = config['target']['bq']['partition_field']
        self.target_bq_cluster_fields            : List[str]             = config['target']['bq']['cluster_fields']
        self.full_target_bq_table                : str                   = f'{self.target_bq_project}.{self.target_bq_dataset}.{self.target_bq_table}'
        self.full_target_bq_table_temp           : str                   = f'{self.target_bq_project}.{self.target_bq_dataset}.{self.target_bq_table}_temp__{{{{ ts_nodash }}}}'

        if self.task_type == POSTGRES_TO_BQ:
            self.quoting = lambda text: f'"{text}"'
            self.string_type = 'TEXT'
            self.date_type = 'DATE'
            self.timestamp_conversion_query = """
                    CAST(CASE
                        WHEN {field_mapping_name} <= '1900-01-01 00:00:00'
                        THEN '1900-01-01 00:00:00'
                        ELSE {field_mapping_name}
                    END AS TIMESTAMP) AS {field_name}
                """
        elif self.task_type == MYSQL_TO_BQ:
            self.quoting = lambda text: f'`{text}`'
            self.string_type = 'CHAR'
            self.date_type = 'DATE'
            self.timestamp_conversion_query = """
                    CASE
                        WHEN {field_mapping_name} <= '1900-01-01 00:00:00'
                        THEN '1900-01-01 00:00:00'
                        ELSE {field_mapping_name}
                    END AS {field_name}
                """
        else:
            raise Exception('Task type is not supported!')

        # Set source_start_window_expansion default value, set both value and unit to 0 and minutes if one of them is not provided
        if self.source_start_window_expansion_value is None or self.source_start_window_expansion_unit is None:
            self.source_start_window_expansion_value = 0
            self.source_start_window_expansion_unit = 'minutes'

        self.base_path_name = self.dag_id.split(".")
        self.dag_base_path = f'{PYTHONPATH}/dags/{self.base_path_name[0]}/{self.base_path_name[1]}'

    def __generate_schema(self, **kwargs) -> list:
        schema_file = os.path.join(self.dag_base_path, "assets/schema.json")

        try:
            logging.info(f'Getting table schema from {schema_file}')
            with open(schema_file, "r") as file:
                schema = json.load(file)

            fields = [schema_detail["name"] for schema_detail in schema]

            # Extend schema from EXTENDED_SCHEMA
            schema.extend(
                [
                    schema_detail
                    for schema_detail in EXTENDED_SCHEMA
                    if schema_detail["name"] not in fields
                ]
            )
        except Exception:
            raise Exception('Failed to generate schema!')

        return schema

    def __generate_extract_query(self, schema: list = None, **kwargs) -> str:
        field_mapping_file = os.path.join(self.dag_base_path, "assets/field_mapping.yaml")
        field_mapping_dict = {}

        if os.path.exists(field_mapping_file):
            with open(field_mapping_file, "r") as file:
                field_mapping = yaml.safe_load(file)

            field_mapping_dict = {entry['mapped_field_name']: entry['original_field_name'] for entry in field_mapping['fields']}

        # Get all field name and exclude the extended field and database field to be selected
        if schema is not None:
            extended_fields = [schema_detail["name"]
                               for schema_detail in EXTENDED_SCHEMA]
            # Wrap extended_fields and DATABASE field into excluded_fields
            excluded_fields = extended_fields
            extended_fields.append(DATABASE) 

            selected_fields = [
                (
                    f"CAST({self.quoting(field_mapping_dict.get(schema_detail['name'], schema_detail['name']))} AS {self.string_type}) AS {self.quoting(schema_detail['name'])}"
                    if schema_detail['type'] == 'STRING'
                    else (
                        f"CAST({self.quoting(field_mapping_dict.get(schema_detail['name'], schema_detail['name']))} AS {self.date_type}) AS {self.quoting(schema_detail['name'])}"
                        if schema_detail['type'] == 'DATE'
                        else (
                            self.timestamp_conversion_query.format(
                                field_mapping_name=self.quoting(field_mapping_dict.get(schema_detail['name'], schema_detail['name'])),
                                field_name=self.quoting(schema_detail['name'])
                            )
                            if schema_detail['type'] == 'TIMESTAMP'
                            else f"{self.quoting(field_mapping_dict.get(schema_detail['name'], schema_detail['name']))}" +
                                (f" AS {self.quoting(schema_detail['name'])}" if schema_detail['name'] in field_mapping_dict else "")
                        )
                    )
                )
                for schema_detail in schema
                if schema_detail["name"] not in excluded_fields
            ]

        # Generate query
        source_extract_query = SOURCE_EXTRACT_QUERY.substitute(
            selected_fields=', '.join([field for field in selected_fields]),
            load_timestamp='CURRENT_TIMESTAMP' if self.task_type == POSTGRES_TO_BQ else 'CURRENT_TIMESTAMP()',
            source_table_name=self.quoting(self.source_table) if self.source_schema is None else f'{self.quoting(self.source_schema)}.{self.quoting(self.source_table)}'
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
                    f"""{self.quoting(timestamp_key)} >=
                    '{{{{ data_interval_start.astimezone(dag.timezone).subtract({self.source_start_window_expansion_unit}={self.source_start_window_expansion_value}) }}}}'
                    AND {self.quoting(timestamp_key)} <
                    '{{{{ data_interval_end.astimezone(dag.timezone) }}}}'"""
                    for timestamp_key in self.source_timestamp_keys
                ]
            )

            # Append extract query condition
            source_extract_query = source_extract_query + f" WHERE {condition}"

        logging.info(f'Extract query: {source_extract_query}')

        return get_onelined_string(source_extract_query)

    def __generate_merge_query(self, schema, **kwargs) -> str:
        table_creation_query_type = 'COPY'
        partition_filter = ''
        query = None

        # Query to get partition_field date list from temp table to be used as partition filter
        if self.target_bq_partition_field is not None:
            temp_table_partition_date_query = TEMP_TABLE_PARTITION_DATE_QUERY.substitute(
                partition_field=self.target_bq_partition_field,
                target_bq_table_temp=self.full_target_bq_table_temp
            )
            partition_filter = f"AND DATE(x.{self.target_bq_partition_field}) IN UNNEST(formatted_dates)"

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

            # Used for table creation if not exists
            # Can't use COPY directly if the merge source is a CTE from the source table
            table_creation_query_type = f'PARTITION BY TIMESTAMP_TRUNC({self.target_bq_partition_field}, DAY) AS'
        else:
            merge_source = f"`{self.full_target_bq_table_temp}`"

        # Check field mapping condition to detect new unique keys that got renamed in source table
        field_mapping_dict = {}
        unique_keys = []
        field_mapping_file = os.path.join(self.dag_base_path, "assets/field_mapping.yaml")
        if os.path.exists(field_mapping_file):
            with open(field_mapping_file, "r") as file:
                    field_mapping = yaml.safe_load(file)
            field_mapping_dict = {entry['original_field_name']: entry['mapped_field_name'] for entry in field_mapping['fields']}

            # Detect unique key names based on field mapping
            for key in self.source_unique_keys:
                if key in field_mapping_dict:
                    unique_keys.append(field_mapping_dict[key])
                else:
                    unique_keys.append(key)
        else:
            unique_keys = self.source_unique_keys

        # Construct delsert query
        if self.target_bq_load_method == DELSERT:
            merge_query = DELSERT_QUERY.substitute(
                table_creation_query_type=table_creation_query_type,
                merge_target=self.full_target_bq_table,
                merge_source=merge_source,
                on_keys=' AND '.join(
                    [f"COALESCE(CAST(x.`{key}` as string), 'NULL') = COALESCE(CAST(y.`{key}` as string), 'NULL')" for key in unique_keys]),
                partition_filter=partition_filter,
                insert_fields=', '.join(
                    [f"`{field['name']}`" for field in schema])
            )

            query = temp_table_partition_date_query + merge_query
            logging.info(f'Delsert query: {query}')

        # Construct upsert query
        elif self.target_bq_load_method == UPSERT:
            merge_query = UPSERT_QUERY.substitute(
                table_creation_query_type=table_creation_query_type,
                merge_target=self.full_target_bq_table,
                merge_source=merge_source,
                on_keys=' AND '.join(
                    [f"COALESCE(CAST(x.`{key}` as string), 'NULL') = COALESCE(CAST(y.`{key}` as string), 'NULL')" for key in unique_keys]),
                partition_filter=partition_filter,
                update_fields=', '.join(
                    [f"x.`{field['name']}` = y.`{field['name']}`" for field in schema]),
                insert_fields=', '.join(
                    [f"`{field['name']}`" for field in schema])
            )

            query = temp_table_partition_date_query + merge_query
            logging.info(f'Upsert query: {query}')

        return get_onelined_string(f'{query}')
    
    def __generate_rdbms_to_gcs_task(self, schema: list, extract_query: str, filename: str, index: int = None):
        # Extract data from Postgres, then load to GCS
        task_id = "extract_and_load_to_gcs" if index is None else f"extract_and_load_to_gcs__{index+1}"

        if self.task_type == POSTGRES_TO_BQ:
            extract = PostgresToGCSOperator(
                task_id                    = task_id,
                postgres_conn_id           = self.source_connection,
                gcp_conn_id                = GCP_CONN_ID,
                sql                        = extract_query,
                bucket                     = GCS_DATA_LAKE_BUCKET,
                export_format              = DestinationFormat.NEWLINE_DELIMITED_JSON,
                filename                   = filename,
                approx_max_file_size_bytes = 200000000,
                write_on_empty             = True,
                schema                     = schema,
                stringify_dict             = True
            )

        # Extract data from MySQL, then load to GCS
        elif self.task_type == MYSQL_TO_BQ:
            extract = MySQLToGCSOperator(
                task_id                    = task_id,
                mysql_conn_id              = self.source_connection,
                gcp_conn_id                = GCP_CONN_ID,
                sql                        = extract_query,
                bucket                     = GCS_DATA_LAKE_BUCKET,
                export_format              = DestinationFormat.NEWLINE_DELIMITED_JSON,
                filename                   = filename,
                approx_max_file_size_bytes = 200000000,
                write_on_empty             = True,
                schema                     = schema,
                stringify_dict             = True
            )

        return extract

    def generate_tasks(self):
        schema = self.__generate_schema()
        ts_nodash = "{{ data_interval_start.astimezone(dag.timezone).strftime('%Y-%m-%d %H:%M:%S') }}"

        # Use WRITE_APPEND if the load method is APPEND, else, use WRITE_TRUNCATE
        write_disposition = WriteDisposition.WRITE_APPEND if self.target_bq_load_method == APPEND else WriteDisposition.WRITE_TRUNCATE

        # TimePartitioning, https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TimePartitioning
        time_partitioning = {
            "type": "DAY",
            "field": self.target_bq_partition_field
        } if self.target_bq_partition_field else None

        # Clustering, https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Clustering
        cluster_fields = self.target_bq_cluster_fields if self.target_bq_cluster_fields else None

        # Task generator for single connection dag
        if type(self.source_connection) is str:
            extract_query = self.__generate_extract_query(schema=schema)
            if "bronze_commerce_new_17" in self.dag_id:
                filename = f'{self.target_bq_dataset}_17/{self.target_bq_table}/job_execution_timestamp={ts_nodash}/{self.source_table}' + '__{}.json'
                self.full_target_bq_table_temp = self.full_target_bq_table_temp + "_17"
            else:
                filename = f'{self.target_bq_dataset}/{self.target_bq_table}/job_execution_timestamp={ts_nodash}/{self.source_table}' + '__{}.json'

            extract = self.__generate_rdbms_to_gcs_task(schema=schema, extract_query=extract_query, filename=filename)

        # Task generator for multiple connection dag
        elif type(self.source_connection) is list:
            extract = []

            for index, connection in enumerate(sorted(self.source_connection)):
                extract_query = self.__generate_extract_query(schema=schema, database=connection)
                if "bronze_commerce_new_17" in self.dag_id:
                    filename = f'{self.target_bq_dataset}_17/{self.target_bq_table}/job_execution_timestamp={ts_nodash}/{self.source_table}_{index+1}' + '__{}.json'
                    self.full_target_bq_table_temp = self.full_target_bq_table_temp + "_17"
                else:
                    filename = f'{self.target_bq_dataset}/{self.target_bq_table}/job_execution_timestamp={ts_nodash}/{self.source_table}_{index+1}' + '__{}.json'

                __extract = self.__generate_rdbms_to_gcs_task(schema=schema, extract_query=extract_query, filename=filename, index=index)

                extract.append(__extract)

        # Directly load the data into BigQuery main table if the load method is TRUNCATE or APPEND, else, load it to temporary table first
        destination_project_dataset_table = f'{self.full_target_bq_table}' if self.target_bq_load_method not in MERGE.__members__ \
            else f'{self.full_target_bq_table_temp}'

        # Load data from GCS to BigQuery
        if "bronze_commerce_new_17" in self.dag_id:
            source_objects = [f'{self.target_bq_dataset}_17/{self.target_bq_table}/job_execution_timestamp={ts_nodash}/*.json']
        else:
            source_objects = [f'{self.target_bq_dataset}/{self.target_bq_table}/job_execution_timestamp={ts_nodash}/*.json']

        load = GCSToBigQueryOperator(
            task_id                           = 'load_to_bq',
            gcp_conn_id                       = GCP_CONN_ID,
            bucket                            = GCS_DATA_LAKE_BUCKET,
            destination_project_dataset_table = destination_project_dataset_table,
            source_objects                    = source_objects,
            schema_fields                     = schema,
            source_format                     = SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition                 = write_disposition,
            time_partitioning                 = time_partitioning,
            cluster_fields                    = cluster_fields,
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
                task_id       = 'merge_to_main_table',
                project_id    = self.target_bq_project,
                gcp_conn_id   = GCP_CONN_ID,
                configuration = configuration
            )

            # Delete temporary table
            delete = BigQueryDeleteTableOperator(
                task_id                = 'delete_temp_table',
                gcp_conn_id            = GCP_CONN_ID,
                deletion_dataset_table = destination_project_dataset_table,
                ignore_if_missing      = True
            )

            # Early return the task flow for MERGE load method
            return extract >> load >> merge >> delete

        # Task flow for TRUNCATE or APPEND method
        return extract >> load
