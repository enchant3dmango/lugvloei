
import logging
import os

import numpy as np
import polars as pl


def dataframe_type_casting(dataframe: pl.DataFrame, schema: list, **kwargs) -> pl.DataFrame:
    """
    Function to cast polars dataframe data types based on provided schema.
    """

    format_date = kwargs.get('format_date', "%Y-%m-%d")
    format_timestamp = kwargs.get('format_timestamp', None)

    if isinstance(format_date, list):
        for each_format_date in format_date:
            for date_field, format_date_key in each_format_date.items():
                dataframe = dataframe.with_columns(date_field, pl.col(
                    date_field).to_date(format=format_date_key))

    if format_timestamp is not None:
        for each_format_timestamp in format_timestamp:
            for timestamp_field, format_timestamp_key in each_format_timestamp.items():
                dataframe = dataframe.with_columns(timestamp_field, pl.col(
                    timestamp_field).to_datetime(format=format_timestamp_key))

    NUMERIC_IGNORED_VALUES = ["", " ", "#REF!", "-", "None"]
    print(f'Dataframe dtypes before casted:\n{dataframe.head(0)}')

    for field in schema:
        field_name, field_type = field['name'], field['type']

        if field_type == "DATE":
            dataframe = dataframe.with_columns(
                pl.col(field_name).cast(
                    pl.Utf8
                ).str.strptime(
                    pl.Date,
                    format='%Y-%m-%d',
                    strict=False,
                    exact=False
                ).cast(
                    pl.Date,
                    strict=False
                )
            )
        elif field_type == "TIMESTAMP":
            dataframe = dataframe.with_columns(
                pl.col(field_name).cast(
                    pl.Utf8
                ).str.strptime(
                    pl.Datetime,
                    format='%Y-%m-%d %H:%M:%S',
                    strict=False,
                    exact=False
                ).cast(
                    pl.Datetime,
                    strict=False
                )
            )
        elif field_type == "TIME":
            dataframe = dataframe.with_columns(
                pl.col(field_name).cast(
                    pl.Utf8
                ).str.strptime(
                    pl.Time,
                    format='%H:%M:%S',
                    strict=False,
                    exact=False
                ).cast(
                    pl.Time,
                    strict=False
                )
            )
        elif field_type == "FLOAT":
            dataframe = dataframe.with_columns(
                pl.col(field_name).map_elements(
                    function=lambda val: np.NaN if val in NUMERIC_IGNORED_VALUES else val,
                    skip_nulls=True,
                    return_dtype=pl.Float64
                )
            )
        elif field_type == "INTEGER":
            dataframe = dataframe.with_columns(
                pl.col(field_name).map_elements(
                    function=lambda val: np.NaN if val in NUMERIC_IGNORED_VALUES else val,
                    skip_nulls=True,
                    return_dtype=pl.Int64
                )
            )
        elif field_type == "BOOLEAN":
            dataframe = dataframe.with_columns(
                pl.col(field_name).cast(dtype=pl.Boolean, strict=False))
        elif field_type == "STRING":
            dataframe = dataframe.with_columns(
                pl.col(field_name).cast(dtype=pl.Utf8, strict=False))

    logging.info(f'Dataframe dtypes after casted:\n{dataframe.head(0)}')

    return dataframe


# TODO: Complete this function, which currently only supports write to parquet
def dataframe_to_file(dataframe: pl.DataFrame, dirname: str, filename: str, **kwargs) -> None:
    extension = str(kwargs.get('extension', 'parquet'))

    # Create local dir if not exists
    dirname = os.path.join('/tmp/', dirname)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    filename = f'{filename}.{extension.lower()}'

    target = os.path.join(dirname, filename)

    logging.info(f'Writing dataframe into {target}.')
    try:
        dataframe.write_parquet(target)
        logging.info(f'Successfully write dataframe into {target}.')
    except:
        raise Exception(f'Failed to write dataframe into {target}.')
