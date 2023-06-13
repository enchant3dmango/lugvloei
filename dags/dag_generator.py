import os
import fnmatch

import yaml

from generators import mysql_to_bq, postgres_to_bq
from plugins.utils.dag_config_reader import get_yaml_config_files
from plugins.constants.variable import BRONZE, MYSQL_TO_BQ, POSTGRES_TO_BQ, SILVER


def get_yaml_config_files(directory, suffix):
    matches = []
    for root, _, filenames in os.walk(directory):
        for filename in fnmatch.filter(filenames, f'*.{suffix}'):
            matches.append(os.path.join(root, filename))
    return matches


def dynamic_dag_generator(config, dag_id):
    if MYSQL_TO_BQ in config.get('dag')['type']:
        mysql_to_bq.generate_dag(config, dag_id)
    elif POSTGRES_TO_BQ in config.get('dag')['type']:
        postgres_to_bq.generate_dag(config, dag_id)


if __name__ == "__main__":

    config_files = get_yaml_config_files(os.getcwd(), '*.yaml')

    for config_file in config_files():
        config = yaml.safe_load(config_file)
        if BRONZE in config.get('dag')['type']:
            dag_id = f'{BRONZE}_{config.get("database")["name"]}.{config.get("database")["table"]}'
        elif SILVER in config.get('dag')['type']:
            dag_id = f'{SILVER}_{config.get("database")["name"]}.{config.get("database")["table"]}'

        dynamic_dag_generator(config=config, dag_id=dag_id)
