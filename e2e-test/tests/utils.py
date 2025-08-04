from common.cluster_utils import DEFAULT_SPARK_CONF

import logging
import os
import time

YT_PROXY = "127.0.0.1:" + os.getenv("PROXY_PORT", "8000")
DRIVER_HOST = "172.17.0.1"

DRIVER_CLIENT_CONF = {
    "spark.driver.host": DRIVER_HOST,
    "spark.driver.port": "27151",
    "spark.ui.port": "27152",
    "spark.blockManager.port": "27153",
}

SPARK_CONF = DEFAULT_SPARK_CONF | {
    "spark.master": "local[4]",
    "spark.hadoop.yt.proxy": YT_PROXY,
    "spark.driver.cores": "1",
    "spark.driver.memory": "768M",
    "spark.ytsaurus.redirect.stdout.to.stderr": "true",
    "spark.ytsaurus.driver.maxFailures": 2,
    "spark.ytsaurus.executor.maxFailures": 2,
} | DRIVER_CLIENT_CONF


def upload_file(yt_client, source_path, remote_path):
    logging.debug(f"Uploading {source_path} to {remote_path}")
    yt_client.create("file", remote_path)
    full_source_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), source_path)
    with open(full_source_path, 'rb') as file:
        yt_client.write_file(remote_path, file)


def wait_for_operation(yt_client, operation_id):
    if operation_id is not None:
        while True:
            current_state = yt_client.get_operation_state(operation_id)
            logging.info(f"Operation: {operation_id}, State: {current_state}")
            if current_state.is_finished():
                return current_state
            time.sleep(1)
