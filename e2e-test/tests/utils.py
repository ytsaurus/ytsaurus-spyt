from common.cluster_utils import DEFAULT_SPARK_CONF

from contextlib import contextmanager
from hashlib import sha256
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
    "spark.hadoop.yt.proxy": YT_PROXY,
    "spark.driver.cores": "1",
    "spark.driver.memory": "768M",
    "spark.ytsaurus.redirect.stdout.to.stderr": "true",
    "spark.ytsaurus.driver.maxFailures": 2,
    "spark.ytsaurus.executor.maxFailures": 2,
} | DRIVER_CLIENT_CONF


def job_path(source_path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), source_path)


def upload_file(yt_client, source_path, remote_path):
    logging.debug(f"Uploading {source_path} to {remote_path}")
    yt_client.create("file", remote_path)
    with open(job_path(source_path), 'rb') as file:
        yt_client.write_file(remote_path, file)


@contextmanager
def temporary_yt_user(yt_client, user_name, token):
    """Create a user that authenticates with the token and remove the user on exit."""
    yt_client.create("user", attributes={"name": user_name}, ignore_existing=True)
    while yt_client.get(f"//sys/users/{user_name}/@life_stage") != "creation_committed":
        time.sleep(1)

    token_hash = sha256(token.encode()).hexdigest()
    yt_client.set(f"//sys/tokens/{token_hash}", user_name)
    yt_client.create("map_node", f"//sys/cypress_tokens/{token_hash}", ignore_existing=True)
    yt_client.set(f"//sys/cypress_tokens/{token_hash}/@user", user_name)
    try:
        yield user_name, token
    finally:
        yt_client.remove(f"//sys/users/{user_name}")
        yt_client.remove(f"//sys/tokens/{token_hash}")
        yt_client.remove(f"//sys/cypress_tokens/{token_hash}")
        while yt_client.exists(f"//sys/users/{user_name}"):
            time.sleep(1)


def get_executors_operation_id(yt_client, driver_operation_id, retries=30):
    for _ in range(retries):
        val = (
            yt_client.get_operation(driver_operation_id)
            .get("runtime_parameters", {})
            .get("annotations", {})
            .get("description", {})
            .get("Executors operation ID")
        )
        if val:
            return val
        time.sleep(1)
    raise TimeoutError("Executors operation ID not found")
