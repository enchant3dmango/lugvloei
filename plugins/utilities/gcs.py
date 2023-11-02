import logging
import os
from google.cloud.storage import Client

client = Client()


def upload_multiple_files_from_local(bucket: str, dirname: str = None, privacy: str = "private"):
    """
    Function to upload multiple local files from a local directory to GCS
    """

    gcs_bucket = client.bucket(bucket)

    # Set local and GCS file path
    local_path = os.path.join('/tmp/', dirname)

    logging.info(f'Reading files from {local_path}.')
    # Upload files to GCS
    for file in os.listdir(local_path):
        # The name of file on GCS once uploaded
        blob = gcs_bucket.blob(f'{local_path}/{file}')
        logging.info(f'Uploading {local_path}/{file} to {blob}.')
        # Path of the local file to upload
        blob.upload_from_filename(f'{local_path}/{file}')

    logging.info(f"Successfully uploaded all files in {os.path.dirname(local_path)} uploaded to GCS.")

    if (privacy != "private"):
        blob.make_public()

    return blob.public_url
