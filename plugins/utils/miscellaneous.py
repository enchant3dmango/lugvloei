import fnmatch
import os
import re


def get_dag_yaml_config_files(directory, suffix):
    """
    Function to read DAG YAML config files.
    """

    matches = []
    for root, _, filenames in os.walk(directory):
        for filename in fnmatch.filter(filenames, f'{suffix}'):
            matches.append(os.path.join(root, filename))

    return matches

def get_parsed_schema_type(schema_type: str) -> str:
    """
    Function to parse the schema type to pandas type in order to specifying dataframe type.
    """

    # Type of parsing
    type = {
        "datetime" : "TIMESTAMP",
        "timestamp": "TIMESTAMP",
        "bool"     : "BOOLEAN",
        "int"      : "INTEGER",
        "float"    : "FLOAT",
        "numeric"  : "FLOAT",
        "double"   : "FLOAT",
        "decimal"  : "FLOAT",
        "time"     : "TIME",
        "date"     : "DATE",
    }

    # Stored and exchange for specific type and their parsing
    for key, value in type.items():
        if key in schema_type:
            return value

    return "STRING"

def get_onelined_format(string: str) -> str:
    """
    Function to convert multi-lined string into one-lined string.
    """

    return re.sub(r'\s+', ' ', string).replace('\n', '')
