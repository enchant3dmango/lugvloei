import os
import re
import sys

import pendulum

sys.path.append(os.getcwd())
from plugins.utilities.generic import (get_config_files,
                                             get_escaped_string,
                                             get_iso8601_date,
                                             get_onelined_string)


def test_get_config_files(mocker):
    # Create a mock for os.walk
    mocker.patch(
        'os.walk',
        return_value=
        [
            (os.path.normpath('/path/to/directory'),
             [],
             ['file-1.txt', 'file-2.txt'])
        ]
        )

    # Replace 'directory' and 'suffix' with your test values
    directory = '/path/to/directory'
    suffix = '*.txt'

    result = get_config_files(os.path.normpath(directory), suffix)
    expected_result = [
        os.path.normpath(f'{directory}/file-1.txt'),
        os.path.normpath(f'{directory}/file-2.txt')
    ]

    # Assert the result
    assert result == expected_result


def test_get_escaped_string():
    string = 'This is a test string.'

    result = get_escaped_string(string)
    expected_result = re.escape(string)

    assert result == expected_result


def test_get_onelined_string():
    string = 'This is\na\nmultiline\nstring.'

    result = get_onelined_string(string)
    expected_result = 'This is a multiline string.'

    assert result == expected_result


def test_get_iso8601_date(mocker):
    fixed_date = pendulum.datetime(2023, 9, 10)
    mocker.patch('pendulum.now', return_value=fixed_date)

    result = get_iso8601_date()
    expected_result = '2023-09-10'

    # Assert the result
    assert result == expected_result

# pytest==7.4.2, pytest-mock==3.11.1
# pytest tests/plugins/utilities/miscellaneous_test.py --verbose