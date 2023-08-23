import fnmatch
import os
import re

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
    Function for get today date
    """

    return pendulum.now().format('YYYY-MM-DD')
