import pytest

import json
import time
from urllib.request import urlopen


def read_json(url):
    with urlopen(url, timeout=30) as response:
        return json.load(response)


def get_stage_ids(sc, job_group, timeout=10):
    tracker = sc.statusTracker()
    deadline = time.time() + timeout

    while time.time() < deadline:
        stage_ids = set()
        for job_id in tracker.getJobIdsForGroup(job_group):
            job_info = tracker.getJobInfo(job_id)
            if job_info:
                stage_ids.update(job_info.stageIds)
        if stage_ids:
            return stage_ids
        time.sleep(0.2)

    raise AssertionError(f"No stages found for job group {job_group}")


def get_stage_metrics(sc, stage_ids, timeout=10):
    if not sc.uiWebUrl:
        raise AssertionError("Spark UI is not available")

    app_id = sc.applicationId
    url = f"{sc.uiWebUrl}/api/v1/applications/{app_id}/stages"
    deadline = time.time() + timeout

    while time.time() < deadline:
        stages = read_json(url)
        stages = [s for s in stages if s["stageId"] in stage_ids and s["status"] == "COMPLETE"]
        if {s["stageId"] for s in stages} >= stage_ids:
            return {
                "bytes_read": sum(s.get("inputBytes", 0) for s in stages),
                "bytes_written": sum(s.get("outputBytes", 0) for s in stages),
                "records_read": sum(s.get("inputRecords", 0) for s in stages),
                "records_written": sum(s.get("outputRecords", 0) for s in stages),
            }
        time.sleep(0.2)

    raise AssertionError(f"No completed metrics for stages {stage_ids}")


def run_with_metrics(session, group, action):
    sc = session.sparkContext
    sc.setJobGroup(group, group)
    result = action()
    metrics = get_stage_metrics(sc, get_stage_ids(sc, group))
    return result, metrics


def assert_close(actual, expected, tolerance=0.2):
    lower = int(expected * (1 - tolerance))
    upper = int(expected * (1 + tolerance))
    assert lower <= actual <= upper, f"{actual} not in [{lower}, {upper}]"


@pytest.mark.parametrize("direct_session", [{"spark.ui.enabled": "true"}], indirect=True)
def test_compare_read_write_bytes(tmp_dir, direct_session):
    row_count: int = 500
    table_path = f"{tmp_dir}/metrics_test"
    data = [(i, i * 2, i * 3) for i in range(row_count)]

    _, write_metrics = run_with_metrics(
        direct_session,
        "write_job",
        lambda: direct_session.createDataFrame(data, ["col1", "col2", "col3"]).write.yt(table_path),
    )

    rows, read_metrics = run_with_metrics(
        direct_session,
        "read_job",
        lambda: direct_session.read.yt(table_path).collect(),
    )
    assert write_metrics["bytes_written"] > 0
    assert write_metrics["records_written"] == row_count

    assert len(rows) == row_count
    assert read_metrics["bytes_read"] > 0
    assert read_metrics["records_read"] == row_count

    assert_close(read_metrics["bytes_read"], write_metrics["bytes_written"])
