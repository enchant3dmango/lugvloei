import json
import os

from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator

from plugins.constants.miscellaneous import (EXTENDED_SCHEMA, MYSQL_TO_BQ,
                                             POSTGRES_TO_BQ, SPARK_JDBC_TASK,
                                             SUBMIT_SPARK_JOB, WRITE_APPEND)


class RdbmsToBq: 
    def __init__(self, dag_id: str, config: dict, **kwargs) -> None: 
        super().__init__(**kwargs)
        self.dag_id                      : str = dag_id
        self.task_type                   : str = config['type']
        self.source_connection           : str = config['source']['connection']
        self.source_schema               : str = config['source']['schema']
        self.source_table                : str = config['source']['table']
        self.source_timestamp_keys       : str = config['source']['timestamp_keys']
        self.source_unique_keys          : str = config['source']['unique_keys']
        self.target_bq_project           : str = config['target']['bq']['project']
        self.target_bq_dataset           : str = config['target']['bq']['dataset']
        self.target_bq_table             : str = config['target']['bq']['table']
        self.target_bq_write_disposition : str = config['target']['bq']['write_disposition']

    def __generate_schema(self, **kwargs) -> list:
        schema_path = f'{os.environ["PYTHONPATH"]}/dags/{self.target_bq_dataset}/{self.target_bq_table}'
        schema_file = os.path.join(schema_path, "assets/schema.json")

        with open(schema_file, "r") as file:
            schema = json.load(file)
            file.close()

        # print(schema)
        fields = [schema_detail["name"] for schema_detail in schema]
        schema.extend(
            [
                schema_detail
                for schema_detail in EXTENDED_SCHEMA
                if schema_detail["name"] not in fields
            ]
        )

        return schema

    def __generate_query(self, schema: list, **kwargs) -> str:
        # Make Function for formating quote and sub checkpoint each connection type task
        if self.task_type == POSTGRES_TO_BQ:

            def quoting(text):
                return f'"{text}"'

        elif self.task_type == MYSQL_TO_BQ:

            def quoting(text):
                return f"`{text}`"

        # Get all field name and the extended field name
        extended_fields = [schema_detail["name"] for schema_detail in EXTENDED_SCHEMA]
        fields = [
            schema_detail["name"]
            for schema_detail in schema
            if schema_detail["name"] not in extended_fields
        ]

        # Generate query
        query = 'SELECT {selected_fields} FROM {source_schema}.{source_table_name}'.format(
            selected_fields=', '.join([quoting(field) for field in fields]),
            source_schema=quoting(self.source_schema),
            source_table_name=quoting(self.source_table),
        )

        # Generate query filter based on write_disposition
        if self.target_bq_write_disposition == WRITE_APPEND:
            # Create the condition for filtering based on timestamp_keys
            condition = ' OR '.join(
                [
                    f'{timestamp_key} >= ' + '{{ data_interval_start.astimezone(dag.timezone) }}' + f' AND {timestamp_key} < ' + '{{ data_interval_end.astimezone(dag.timezone) }}'
                    for timestamp_key in self.source_timestamp_keys
                ]
            )
            query += f" WHERE {condition}"

        return query

    def generate_schema(self) -> list:
        return self.__generate_schema()
    
    def generate_task(self) -> SparkSubmitOperator:
        print(self.__generate_query(schema=self.__generate_schema()))
        return SparkSubmitOperator(
            application      = "",
            application_args = [
                '--query', self.__generate_query(schema=self.__generate_schema()),
                '--type', self.task_type,
            ],
            conn_id = "",
            name    = f'{SPARK_JDBC_TASK}_',
            task_id = SUBMIT_SPARK_JOB,
        )
