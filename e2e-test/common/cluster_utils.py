from spyt.standalone import find_spark_cluster

import logging
import os
from pathlib import Path
import requests
import shutil


logger = logging.getLogger(__name__)


def dump_debug_data(dump_dir=None, operation=None, yt_root_path=None, yt_client=None, discovery_path=None):
    if not dump_dir:
        logger.info("Directory for preserving debug data is not provided")
        return

    if operation:
        dump_operation_logs(dump_dir, operation)
    if yt_root_path:
        dump_cluster_logs(dump_dir, yt_root_path)
    if yt_client and discovery_path:
        cluster = find_spark_cluster(discovery_path, yt_client)
        if cluster.master_web_ui_url:
            dump_html_page(dump_dir, cluster.master_web_ui_url, "spark_webui")
        if cluster.livy_url:
            dump_livy_logs(dump_dir, yt_root_path, cluster.livy_url)


def get_data_path(dump_dir, relative_path, create_dir=False):
    path = os.path.join(dump_dir, relative_path)
    if create_dir:
        Path(path).mkdir(parents=True, exist_ok=True)
    return path


def dump_runtime_files(dump_dir, yt_root_path, subdir, lbl, ignore_function):
    logger.info(f"Trying dump runtime data from {yt_root_path}/<slot-runtime-data>/{subdir}")
    if not yt_root_path:
        logger.warn("No yt cluster root path found")
        return

    nodes_path = os.path.join(yt_root_path, "runtime_data", "node")
    nodes_list = os.listdir(nodes_path)
    logger.debug(f"Found {len(nodes_list)} nodes")
    for node_idx in nodes_list:
        slots_path = os.path.join(nodes_path, node_idx, "slots")
        slots_list = os.listdir(slots_path)
        logger.debug(f"Found {len(slots_list)} slots in node {node_idx} directory")
        for slot_idx in slots_list:
            slot_path = os.path.join(slots_path, slot_idx)
            candidates = [os.path.join(slot_path, "sandbox", subdir), os.path.join(slot_path, "user", subdir)]
            for source_logs_path in candidates:
                if not os.path.exists(source_logs_path):
                    continue
                logger.info(f"{source_logs_path} found, copying to persistent storage")
                shutil.copytree(source_logs_path, get_data_path(dump_dir, f"{lbl}_{node_idx}_{slot_idx}"),
                                ignore=ignore_function)


def dump_cluster_logs(dump_dir, yt_root_path):
    def ignore_function(directory, contents):
        return [f for f in contents if os.path.isfile(os.path.join(directory, f)) and f not in ["stderr", "stdout"]]

    dump_runtime_files(dump_dir, yt_root_path, os.path.join("spark", "work"), "worker", ignore_function)


def dump_operation_logs(dump_dir, operation):
    dest_logs_path = get_data_path(dump_dir, "operation_logs", create_dir=True)
    job_infos = operation.get_jobs_with_error_or_stderr()
    for i, job_info in enumerate(job_infos):
        Path(dest_logs_path, f'stderr_{i}').write_text(job_info['stderr'])


def dump_html_page(dump_dir, address, component):
    dest_html_path = get_data_path(dump_dir, "ui_pages", create_dir=True)
    try:
        page_html = requests.get("http://" + address, timeout=(3, 3)).text
        Path(dest_html_path, component + '.html').write_text(page_html)
    except Exception:
        logger.info(f"HTML page ({address}) is not available", exc_info=True)


def dump_livy_logs(dump_dir, yt_root_path, livy_url):
    if not livy_url:
        logger.warn("Livy server is not defined, logs will not be saved")

    dump_html_page(dump_dir, livy_url + "/ui", "livy_ui")

    dump_runtime_files(dump_dir, yt_root_path, os.path.join("livy", "logs"), "livy", ignore_function=None)

    dest_logs_path = get_data_path(dump_dir, "livy_session_logs", create_dir=True)
    for i in range(5):
        try:
            response = requests.get(f"http://{livy_url}/sessions/{i}/log?size=-1")
            if response.status_code == 200:
                logs = response.json()['log']
                Path(dest_logs_path, f'stderr_{i}').write_text('\n'.join(logs))
        except Exception:
            logger.debug(f"Error in getting logs for session#{i}", exc_info=True)


def is_accessible(url):
    if not url:
        return False
    try:
        return requests.get("http://" + url, allow_redirects=True).ok
    except Exception as e:
        logger.debug(f"Failed accessible query: {e}")
        return False
