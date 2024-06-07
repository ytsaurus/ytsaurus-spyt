from spyt.submit import SubmissionStatus

from common.helpers import assert_items_equal
from utils import upload_file


def test_cluster_mode(yt_client, tmp_dir, spyt_cluster):
    table_in = f"{tmp_dir}/t_in"
    table_out = f"{tmp_dir}/t_out"
    yt_client.create("table", table_in, attributes={"schema": [{"name": "a", "type": "int64"}]})
    rows = [{"a": i} for i in range(10)]
    yt_client.write_table(table_in, rows)

    upload_file(yt_client, 'jobs/id.py', f'{tmp_dir}/id.py')

    status = spyt_cluster.submit_cluster_job(f'{tmp_dir}/id.py', args=[table_in, table_out])
    assert status is SubmissionStatus.FINISHED

    assert_items_equal(yt_client.read_table(table_out), rows)


def test_client_mode(yt_client, tmp_dir, cluster_session):
    table_in = f"{tmp_dir}/client_in"
    table_out = f"{tmp_dir}/client_out"
    yt_client.create("table", table_in, attributes={"schema": [{"name": "a", "type": "int64"}]})
    rows = [{"a": i} for i in range(10)]
    yt_client.write_table(table_in, rows)

    df = cluster_session.read.yt(table_in)
    df.write.yt(table_out)

    assert_items_equal(yt_client.read_table(table_out), rows)


def test_cluster_mode_with_dependencies(yt_client, tmp_dir, spyt_cluster):
    table_in = f"{tmp_dir}/t_dep_in"
    table_out = f"{tmp_dir}/t_dep_out"
    yt_client.create("table", table_in, attributes={"schema": [{"name": "a", "type": "int64"}]})
    rows = [{"a": i} for i in range(105)]
    yt_client.write_table(table_in, rows)

    for script in ['job_with_dependencies.py', 'dependencies.py']:
        upload_file(yt_client, f'jobs/{script}', f'{tmp_dir}/{script}')

    status = spyt_cluster.submit_cluster_job(f'{tmp_dir}/job_with_dependencies.py', args=[table_in, table_out],
                                             py_files=[f'{tmp_dir}/dependencies.py'])
    assert status is SubmissionStatus.FINISHED

    result_rows = [
        {"key": "Reminder count for 0", "count": 11},
        {"key": "Reminder count for 1", "count": 11},
        {"key": "Reminder count for 2", "count": 11},
        {"key": "Reminder count for 3", "count": 11},
        {"key": "Reminder count for 4", "count": 11},
        {"key": "Reminder count for 5", "count": 10},
        {"key": "Reminder count for 6", "count": 10},
        {"key": "Reminder count for 7", "count": 10},
        {"key": "Reminder count for 8", "count": 10},
        {"key": "Reminder count for 9", "count": 10}
    ]
    assert_items_equal(yt_client.read_table(table_out), result_rows)
