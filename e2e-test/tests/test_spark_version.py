import os

import spyt # noqa
import pytest
from common.cluster import direct_spark_session
from spyt.submit import direct_submit
from pyspark import __version__ as current_version

from utils import YT_PROXY, upload_file
from common.helpers import wait_for_operation


def _get_spark_version_from_stderr(yt_client, operation_id):
    wait_for_operation(yt_client, operation_id)
    jobs = list(yt_client.list_jobs(operation_id)['jobs'])
    if not jobs:
        return None

    for job in jobs:
        job_id = job['id']
        try:
            stderr_bytes = yt_client.get_job_stderr(operation_id, job_id).read()
            stderr = stderr_bytes.decode('utf-8', errors='ignore')
            for line in stderr.splitlines():
                if "SPARK_VERSION=" in line:
                    return line.split("SPARK_VERSION=")[1].strip()
        except Exception:
            continue
    return None


@pytest.fixture
def different_spark_version():
    available_versions = os.environ.get('SPARK_VERSIONS', '').split()
    available_versions = [v.strip() for v in available_versions if v.strip()]

    if not available_versions:
        pytest.skip("SPARK_VERSIONS environment variable not set")

    for version in available_versions:
        if version != current_version:
            return version

    pytest.skip(f"No different Spark version found. Current: {current_version}, Available: {available_versions}")


@pytest.mark.two_spark
def test_direct_submit_with_different_version(yt_client, tmp_dir, different_spark_version):
    upload_file(yt_client, 'jobs/spark_version_job.py', f'{tmp_dir}/spark_version_job.py')

    test_version = different_spark_version

    operation_id = direct_submit(
        yt_proxy=YT_PROXY,
        num_executors=1,
        main_file=f"yt:/{tmp_dir}/spark_version_job.py",
        deploy_mode="cluster",
        spark_conf={
            "spark.ytsaurus.spark.version": test_version,
            "spark.hadoop.yt.token": "token"
        }
    )

    assert operation_id is not None, "direct_submit should return operation_id"

    driver_version = _get_spark_version_from_stderr(yt_client, operation_id)
    assert driver_version == test_version, \
        f"Expected Spark version {test_version} in driver stderr, but got {driver_version}"


@pytest.mark.two_spark
def test_direct_submit_client_mode_with_version_error():
    with pytest.raises(Exception) as exc_info:
        with direct_spark_session(YT_PROXY, extra_conf={"spark.ytsaurus.spark.version": "3.2.1"}) as session:
            session.range(1).collect()

    error_str = str(exc_info.value)
    error_repr = repr(exc_info.value)
    full_error = (error_str + " " + error_repr).lower()
    assert "only supported in cluster mode" in full_error or "spark.ytsaurus.spark.version" in full_error, \
        f"Expected error message about cluster mode, got: {exc_info.value}"
