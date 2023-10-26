import fnmatch
import logging
import os
import re

import numpy as np
import pandas as pd
import pendulum


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

    format_date = "%Y-%m-%d" if kwargs.get(
        'format_date') == None else kwargs.get('format_date')
    format_timestamp = None if kwargs.get(
        'format_timestamp') == None else kwargs.get('format_timestamp')

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
                ["", " ", "#REF!", "-", "None"], np.NaN).astype("float")
            dataframe[field_name] = pd.to_numeric(dataframe[field_name])

        elif field_type == "INTEGER":
            dataframe[field_name] = dataframe[field_name].replace(
                ["", " ", "#REF!", "-", "None"], np.NaN)
            dataframe[field_name] = dataframe[field_name].astype("Int64")

        elif field_type == "BOOLEAN":
            dataframe[field_name] = dataframe[field_name].astype("bool")

        elif field_type == "STRING":
            dataframe[field_name] = dataframe[field_name].astype(str)

    logging.info(dataframe.dtypes)

    return dataframe