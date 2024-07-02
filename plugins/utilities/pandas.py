import logging
import os

import numpy as np
import pandas as pd


def get_casted_dataframe(dataframe: pd.DataFrame, schema: list, **kwargs) -> pd.DataFrame:
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

    if format_timestamp is not None and not isinstance(format_timestamp, str):
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
        elif field_type == "TIMESTAMP" and (format_timestamp is None or isinstance(format_timestamp, str)):
            utc = True if format_timestamp else False
            dataframe[field_name] = pd.to_datetime(
                dataframe[field_name], errors="coerce", utc=utc, format=format_timestamp)
        elif field_type == "FLOAT":
            dataframe[field_name] = pd.to_numeric(dataframe[field_name].astype(
                str).replace(["", " ", "#REF!", "-", "None", "nan"], np.NaN, regex=True)).astype(float)
        elif field_type == "INTEGER":
            dataframe[field_name] = pd.to_numeric(dataframe[field_name].replace(
                ["", " ", "#REF!", "-", "None", "nan"], np.NaN)).astype('Int64')
        elif field_type == "BOOLEAN":
            dataframe[field_name] = dataframe[field_name].astype(bool)
        elif field_type == "STRING":
            dataframe[field_name] = dataframe[field_name].astype(
                str).replace(["", " ", "#REF!", "-", "None", "nan"], None)

    logging.info(f'Dataframe dtypes after casted:\n{dataframe.dtypes}')

    return dataframe


def get_dataframe_file(dataframe: pd.DataFrame, dirname: str, filename: str, extension: str, **kwargs) -> None:
    """
    Function to write dataframe into a file based on provided schema.
    """

    # Create local dir if not exists
    dirname = os.path.join('/tmp/', dirname)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    filename = os.path.join(dirname, filename)
    logging.info(f'Writing dataframe into {extension} file to {filename}.')

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

    return
