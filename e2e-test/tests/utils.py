import logging
import os


def upload_file(yt_client, source_path, remote_path):
    logging.debug(f"Uploading {source_path} to {remote_path}")
    yt_client.create("file", remote_path)
    full_source_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), source_path)
    with open(full_source_path, 'rb') as file:
        yt_client.write_file(remote_path, file)
