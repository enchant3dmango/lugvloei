import fnmatch
import os


def get_dag_yaml_config_files(directory, suffix):
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
    conf = {
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
    for key, values in conf.items():
        if key in schema_type:
            return values
    
    return "STRING"