import json
import logging
import os
import shutil


def delete_file(filename: str) -> None:
    """
    Function to delete file.
    """

    logging.info(f"Removing all files in {os.path.join('/tmp/', filename)}.")
    os.remove(os.path.join('/tmp/', filename))

    logging.info(f"Successfully remove {os.path.join('/tmp/', filename)}.")

    return


def delete_multiple_files(dirname: str) -> None:
    """
    Function to delete a directory, and its subdirectory and files.
    """

    logging.info(f"Removing {os.path.join('/tmp/', dirname)}")
    shutil.rmtree(os.path.join('/tmp/', dirname))
    logging.info(
        f"Successfully remove all files in {os.path.join('/tmp/', dirname)}.")

    return


def get_query_from_file(dirname: str, filename: str, vars: dict = {}) -> str:
    """
    Function to get query string from a file.
    """

    with open(os.path.join(dirname, filename), "r") as f:
        query = f.read()

        for key, value in vars.items():
            query = query.replace(f"{{{key}}}", value)

    return query


def get_schema_from_file(dirname: str, filename: str, extended_fields: list = None) -> list:
    """
    Function to get schema dictionary from a file.
    """

    try:
        logging.info(f'Getting table schema from {os.path.join(dirname, filename)}')
        with open(os.path.join(dirname, filename), "r") as file:
            schema = json.load(file)

        if extended_fields is not None:
            fields = [schema_detail["name"] for schema_detail in schema]

            # Extend schema from extended_fields
            schema.extend(
                [
                    schema_detail
                    for schema_detail in extended_fields
                    if schema_detail["name"] not in fields
                ]
            )

    except Exception:
        raise Exception('Failed to generate schema!')

    return schema
