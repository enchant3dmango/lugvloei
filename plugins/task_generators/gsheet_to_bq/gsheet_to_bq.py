import json
import logging
import os
from typing import List

from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.cloud.bigquery import (CreateDisposition, SourceFormat,
                                   WriteDisposition)
from google.oauth2 import service_account
from googleapiclient import discovery

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from plugins.constants.types import AIRFLOW, APPEND, EXTENDED_SCHEMA, MERGE
from plugins.constants.variables import GCP_CONN_ID, GCS_DATA_LAKE_BUCKET
from plugins.utilities.gcs import upload_multiple_files_from_local
from plugins.utilities.general import (dataframe_dtypes_casting,
                                       dataframe_to_file, remove_file)


class GSheetToBQGenerator:
    """
    Constructor arguments:
        dag_id (Required: String):
            The ID of the Directed Acylic Graph (DAG) to which the generated tasks belog.

        config (Required: Dictionary):
            A dictionary containing configuration parameters for the data transfer process.

        **kwargs:
            Additional keyword arguments
    """

    def __init__(self, dag_id: str, config: dict, **kwargs) -> None:
        self.dag_id                    : str        = dag_id
        self.task_type                 : str        = config['type']
        self.task_mode                 : str        = config['mode']
        self.spreadsheet_id            : str        = config['source']['spreadsheet_id']
        self.sheet_name                : str        = config['source']['sheet_name']
        self.sheet_columns             : str        = config['source']['sheet_columns']
        self.format_date               : List[dict] = config['source']['format_date']
        self.format_timestamp          : List[dict] = config['source']['format_timestamp']
        self.target_bq_project         : str        = config['target']['bq']['project']
        self.target_bq_dataset         : str        = config['target']['bq']['dataset']
        self.target_bq_table           : str        = config['target']['bq']['table']
        self.target_bq_load_method     : str        = config['target']['bq']['load_method']
        self.target_bq_partition_field : str        = config['target']['bq']['partition_field']
        self.target_bq_cluster_fields  : List[str]  = config['target']['bq']['cluster_fields']
        self.full_target_bq_table      : str        = f'{self.target_bq_project}.{self.target_bq_dataset}.{self.target_bq_table}'
        self.full_target_bq_table_temp : str        = f'{self.target_bq_project}.{self.target_bq_dataset}.{self.target_bq_table}_temp__{{{{ ts_nodash }}}}'

        # Define dag base path and default file extension for each dag
        self.dag_base_path = f'{os.environ["PYTHONPATH"]}/dags/{self.target_bq_dataset}/{self.target_bq_table}'
        self.extension = 'parquet'

    def __generate_schema(self) -> list:
        schema_file = os.path.join(self.dag_base_path, "assets/schema.json")

        try:
            logging.info(f'Getting table schema from {schema_file}')

            with open(schema_file, "r") as file:
                schema = json.loads(file.read())

            fields = [schema_detail["name"] for schema_detail in schema]

            # extend schema from EXTENDED_SCHEMA
            schema.extend(
                [
                    schema_detail
                    for schema_detail in EXTENDED_SCHEMA
                    if schema_detail['name'] not in fields
                ]
            )

        except:
            raise Exception("Failed to generate schema!")

        return schema

    # Function to generate and extract data into parquet file
    def __extract_data_from_spreadsheet(self, **kwargs):
        # Extract kwargs
        schema = kwargs.get('schema', None)
        dirname = kwargs.get('dirname', None)
        filename = kwargs.get('filename', None)
        format_date = kwargs.get('format_date', None)
        format_timestamp = kwargs.get('format_timestamp', None)

        # Define credentials for create service API
        with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'r') as file:
            credential_info = file.read()

        creds = json.loads(credential_info)
        credentials = service_account.Credentials.from_service_account_info(
            creds,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/cloud-platform"
            ]
        )

        # Initiate the Sheets API
        service = discovery.build('sheets', 'v4', credentials=credentials)

        # Read the data from spreadsheet
        if "," in self.sheet_columns:
            specific_column = self.sheet_columns.split(",")
            specific_ranges = [f"'{self.sheet_name}'!{columns}" for columns in specific_column]
        else:
            specific_ranges = [f"'{self.sheet_name}'!{self.sheet_columns}"]

        list_of_dataframe = []
        for range_data in specific_ranges:
            values_sheet = service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_data,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="FORMATTED_STRING"
            ).execute()

            values = values_sheet['values']
            specific_df = pd.DataFrame(values)
            list_of_dataframe.append(specific_df)

        # Get column name to define the column name on dataframe
        column_name = [schema_data['name'] for schema_data in schema]

        # Generate dataframe
        dataframe = pd.concat(list_of_dataframe, axis=1)
        dataframe.columns = column_name[:-1]
        dataframe = dataframe.drop(0)  # Remove old column name
        dataframe['load_timestamp'] = datetime.now()

        # Transform data type based on schema
        dataframe = dataframe_dtypes_casting(
            dataframe=dataframe,
            schema=schema, 
            format_date=format_date, 
            format_timestamp=format_timestamp
        )
        logging.info(dataframe.head(1))

        # Upload dataframe files to GCS
        if not dataframe.empty:
            dataframe_to_file(
                dataframe=dataframe,
                dirname=dirname,
                filename=filename,
                extension=f'.{self.extension}'
            )

            upload_multiple_files_from_local(
                bucket=GCS_DATA_LAKE_BUCKET,
                dirname=dirname,
            )

            remove_file(
                filename=f'{dirname}/{filename}'
            )

    def __check_if_file_exists_in_gcs(self, **kwargs):
        bucket = kwargs.get('bucket', None)
        name = f"{kwargs.get('dirname', None)}/{kwargs.get('filename', None)}"

        # Initiate GCS client
        client = storage.Client()
        gcs_bucket = client.bucket(bucket_name=bucket)

        # Determine next task to be executed based on file existence
        file_exists = storage.Blob(name=name, bucket=gcs_bucket).exists()
        if file_exists:
            logging.info("File exists, executing load_to_bq task.")
            return f"load_to_bq"
        else:
            logging.info("File not exists, executing end_pipeline task.")
            return f'end_pipeline'

    def generate_task(self):
        if self.task_mode == AIRFLOW:
            schema = self.__generate_schema()

            # Generate bucket directory name and filename
            ts_nodash = "{{ ts_nodash }}"
            dirname = f'{self.target_bq_dataset}/{self.target_bq_table}/{ts_nodash}'
            filename = f'{self.target_bq_table}.{self.extension}'

            # Use WRITE_APPEND if the load method is APPEND, else, use WRITE_TRUNCATE
            write_disposition = WriteDisposition.WRITE_APPEND if self.target_bq_load_method == APPEND else WriteDisposition.WRITE_TRUNCATE

            # TimePartitioning, https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TimePartitioning
            time_partitioning = {
                "type": "DAY",
                "field": self.target_bq_partition_field
            } if self.target_bq_partition_field else None

            # Clustering, https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Clustering
            cluster_fields = self.target_bq_cluster_fields if self.target_bq_cluster_fields else None

            extract = PythonOperator(
                task_id=f"extract_and_load_to_gcs",
                python_callable=self.__extract_data_from_spreadsheet,
                op_kwargs={
                    "schema": schema,
                    "dirname": dirname,
                    "filename": filename,
                    "format_date": self.format_date,
                    "format_timestamp": self.format_timestamp
                }
            )

            branch_task = BranchPythonOperator(
                task_id=f'check_if_file_exists_in_gcs',
                python_callable=self.__check_if_file_exists_in_gcs,
                op_kwargs={
                    'bucket': GCS_DATA_LAKE_BUCKET,
                    'dirname': dirname,
                    'filename': filename
                }
            )

            # Directly load the data into BigQuery main table if the load method is TRUNCATE or APPEND, else, load it to temporary table first
            destination_project_dataset_table = f'{self.full_target_bq_table}' if self.target_bq_load_method not in MERGE.__members__ \
                else f'{self.full_target_bq_table_temp}'

            load = GCSToBigQueryOperator(
                task_id                           = f'load_to_bq',
                gcp_conn_id                       = GCP_CONN_ID,
                bucket                            = GCS_DATA_LAKE_BUCKET,
                destination_project_dataset_table = destination_project_dataset_table,
                source_objects                    = [f'{self.target_bq_dataset}/{self.target_bq_table}/{ts_nodash}/*.{self.extension}'],
                schema_fields                     = schema,
                source_format                     = SourceFormat.PARQUET,
                write_disposition                 = write_disposition,
                create_disposition                = CreateDisposition.CREATE_IF_NEEDED,
                time_partitioning                 = time_partitioning,
                cluster_fields                    = cluster_fields,
                autodetect                        = False
            )

            end_pipeline = EmptyOperator(task_id='end_pipeline')

            return extract >> branch_task >> [load, end_pipeline]
