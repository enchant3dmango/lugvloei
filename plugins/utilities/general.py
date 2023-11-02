import fnmatch
import logging
import os
import re

import numpy as np
import pandas as pd
import pendulum
import polars as pl


def get_config_files(directory, suffix):
    """
    Function to read config files based on directory and filename suffix.
    """

    matches = []
    for root, _, filenames in os.walk(directory):
        for filename in fnmatch.filter(filenames, f'{suffix}'):
            matches.append(os.path.join(root, filename))

    return matches


def get_escaped_string(string: str) -> str:
    """
    Function to escape string.
    """

    return re.escape(string)


def get_onelined_string(string: str) -> str:
    """
    Function to convert multi-lined string into one-lined string.
    """

    return re.sub(r'\s+', ' ', string).replace('\n', '')


def get_iso8601_date():
    """
    Function to get today date in ISO8601 format.
    """

    return pendulum.now().format('YYYY-MM-DD')


def dataframe_dtypes_casting(dataframe: pd.DataFrame, schema: list, **kwargs) -> pd.DataFrame:
    """
    Function to cast dataframe data types based on provided schema.
    """

    format_date = kwargs.get('format_date', "%Y-%m-%d")
    format_timestamp = kwargs.get('format_timestamp', None)

    if isinstance(format_date, list):
        for each_format_date in format_date:
            for date_field, format_date_key in each_format_date.items():
                dataframe[date_field] = pd.to_datetime(
                    dataframe[date_field], errors="coerce", utc=True, format=format_date_key).dt.date

    if format_timestamp != None:
        for each_format_timestamp in format_timestamp:
            for timestamp_field, format_timestamp_key in each_format_timestamp.items():
                dataframe[timestamp_field] = pd.to_datetime(
                    dataframe[timestamp_field], errors="coerce", utc=True, format=format_timestamp_key)

    for field in schema:
        field_name = field['name']
        field_type = field['type']

        if field_type == "DATE" and isinstance(format_date, str):
            dataframe[field_name] = pd.to_datetime(
                dataframe[field_name], errors="coerce", utc=True, format=format_date).dt.date
        elif field_type == "TIMESTAMP" and (format_timestamp == None or isinstance(format_timestamp, str)):
            if format_timestamp == None:
                dataframe[field_name] = pd.to_datetime(
                    dataframe[field_name], errors="coerce")
            else:
                dataframe[field_name] = pd.to_datetime(
                    dataframe[field_name], errors="coerce", utc=True, format=format_timestamp)
        elif field_type == "FLOAT":
            dataframe[field_name] = dataframe[field_name].astype(str).replace(
                ["", " ", "#REF!", "-", "None"], np.NaN).astype(float)
            dataframe[field_name] = pd.to_numeric(dataframe[field_name])
        elif field_type == "INTEGER":
            dataframe[field_name] = dataframe[field_name].replace(
                ["", " ", "#REF!", "-", "None"], np.NaN)
            dataframe[field_name] = dataframe[field_name].astype('int64')
        elif field_type == "BOOLEAN":
            dataframe[field_name] = dataframe[field_name].astype(bool)
        elif field_type == "STRING":
            dataframe[field_name] = dataframe[field_name].astype(str)

    logging.info(f'Dataframe dtypes after casted:\n{dataframe.dtypes}')

    return dataframe


def dataframe_to_file(dataframe: pd.DataFrame, filename: str, extension: str, **kwargs) -> None:
    # Create local /tmp/ dir if not exists
    if not os.path.exists('/tmp/'):
        os.makedirs('/tmp/')

    filename = os.path.join('/tmp/', filename)

    if extension == '.gz':
        dataframe.to_json(path_or_buf=filename, orient='records', lines=kwargs.get(
            'lines'), force_ascii=False, date_format='iso', compression='gzip')
    elif extension == '.json':
        dataframe.to_json(path_or_buf=filename, orient='records', lines=kwargs.get(
            'lines'), force_ascii=False, date_format='iso')
    elif extension == '.csv':
        dataframe.to_csv(path_or_buf=filename, sep=kwargs.get(
            'delimiter'), quotechar=kwargs.get('quotechar'), index=False)
    elif extension == '.parquet':
        dataframe.to_parquet(path=filename)
    else:
        raise Exception('Extension is not supported!')


def delete_directory(dirname: str) -> None:
    os.removedirs(dirname)


def polars_dataframe_type_mapping(dataframe: pl.DataFrame, schema: list, **kwargs) -> pl.DataFrame:
    # Define a mapping of BigQuery data types to Polars data types
    type_mapping = {
        "STRING": pl.Utf8,
        "BOOLEAN": pl.Boolean,
        "INTEGER": pl.Int64,
        "FLOAT": pl.Float64,
        "DATE": pl.Date,
        "TIME": pl.Time,
        "TIMESTAMP": pl.Datetime,
    }

    # Cast the Polars DataFrame schema based on the schema provided
    dataframe = dataframe.select(
        [pl.col(field["name"]).cast(type_mapping[field["type"]])
         for field in schema]
    )

    return dataframe


# TODO: Complete this function
def polars_dataframe_format(dataframe: pl.DataFrame, **kwargs) -> None:
    dataframe.write_parquet(kwargs.get('filepath_temp', '/tmp/'))
