import fnmatch
import os


def get_dag_yaml_config_files(directory, suffix):
    matches = []
    for root, _, filenames in os.walk(directory):
        for filename in fnmatch.filter(filenames, f'{suffix}'):
            matches.append(os.path.join(root, filename))

    return matches
