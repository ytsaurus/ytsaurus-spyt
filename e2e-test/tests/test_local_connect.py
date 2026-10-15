from concurrent import futures
from contextlib import ExitStack
from io import BytesIO
from queue import Queue
import os
import socket
import subprocess
import time

import grpc
import pyarrow as pa
import pytest
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.connect.functions as f
from pyspark.sql.connect.proto import base_pb2, base_pb2_grpc, types_pb2
from pyspark.sql.types import Row
from spyt.utils import get_spyt_conf_dir


class _ForwardingStub(base_pb2_grpc.SparkConnectServiceServicer):
    def __init__(self, forwarded):
        self.forwarded = forwarded
        arrow_data = BytesIO()
        arrow_table = pa.table({"message": ["stub"]})
        with pa.ipc.new_stream(arrow_data, arrow_table.schema) as writer:
            writer.write_table(arrow_table)
        self.arrow_batch = base_pb2.ExecutePlanResponse.ArrowBatch(row_count=1, data=arrow_data.getvalue())
        self.schema = types_pb2.DataType(struct=types_pb2.DataType.Struct(fields=[
            types_pb2.DataType.StructField(
                name="message", data_type=types_pb2.DataType(string=types_pb2.DataType.String()), nullable=False,
            ),
        ]))

    def ExecutePlan(self, request, context):
        plan_kind = request.plan.WhichOneof("op_type")
        command_kind = request.plan.command.WhichOneof("command_type") if plan_kind == "command" else None
        self.forwarded.put((plan_kind, command_kind))
        response = dict(
            session_id=request.session_id,
            server_side_session_id=request.client_observed_server_side_session_id,
            operation_id=request.operation_id,
        )
        yield base_pb2.ExecutePlanResponse(**response, schema=self.schema, arrow_batch=self.arrow_batch)
        yield base_pb2.ExecutePlanResponse(**response, result_complete=base_pb2.ExecutePlanResponse.ResultComplete())


def _stop_connect_process(process):
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


@pytest.fixture
def local_forwarding_session(tmp_path):
    forwarded = Queue()
    spark_home = os.path.dirname(pyspark.__file__)
    with ExitStack() as cleanup:
        stub_server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        base_pb2_grpc.add_SparkConnectServiceServicer_to_server(_ForwardingStub(forwarded), stub_server)
        stub_port = stub_server.add_insecure_port("127.0.0.1:0")
        stub_server.start()
        cleanup.callback(lambda: stub_server.stop(grace=0).wait())

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            connect_port = sock.getsockname()[1]
        log_path = tmp_path / "connect-server.log"
        log_file = cleanup.enter_context(log_path.open("w"))
        environment = {**os.environ, "SPARK_HOME": spark_home, "SPARK_CONF_DIR": get_spyt_conf_dir()}
        settings = {
            "spark.connect.grpc.binding.port": connect_port,
            "spark.blockManager.port": 0,
            "spark.ui.enabled": "false",
            "spark.ytsaurus.connect.forward.methods": "ExecutePlan.root,ExecutePlan.command.sql_command",
            "spark.ytsaurus.connect.forward.mirror.methods": "ExecutePlan.command.create_dataframe_view",
            "spark.ytsaurus.connect.forward.endpoint": f"127.0.0.1:{stub_port}",
        }
        command = [
            os.path.join(spark_home, "bin", "spark-submit"),
            "--master", "local[4]",
            "--class", "org.apache.spark.sql.connect.ytsaurus.SpytConnectServer",
            *(arg for key, value in settings.items() for arg in ("--conf", f"{key}={value}")),
            "spark-internal",
        ]
        process = subprocess.Popen(
            command, env=environment, stdout=log_file, stderr=subprocess.STDOUT,
        )
        cleanup.callback(_stop_connect_process, process)

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                pytest.fail(f"Local Spark Connect server exited early:\n{log_path.read_text()}")
            try:
                with socket.create_connection(("127.0.0.1", connect_port), timeout=1):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            pytest.fail(f"Local Spark Connect server did not start:\n{log_path.read_text()}")

        spark = SparkSession.builder.remote(f"sc://127.0.0.1:{connect_port}").getOrCreate()
        cleanup.callback(os.environ.pop, "SPARK_CONNECT_MODE_ENABLED", None)
        cleanup.callback(spark.stop)
        yield spark, forwarded


def test_local_connect_request_forwarding(local_forwarding_session):
    spark, forwarded = local_forwarding_session
    result = spark.range(100).groupBy(f.col("id") % 10).count().collect()
    assert result == [Row(message="stub")]
    assert forwarded.get(timeout=10) == ("root", None)

    assert spark.conf.get("spark.app.name")
    assert forwarded.empty()

    spark.range(2).createOrReplaceTempView("forwarding_test_view")
    assert forwarded.get(timeout=10) == ("command", "create_dataframe_view")
    assert spark.table("forwarding_test_view").schema.fieldNames() == ["id"]
    assert forwarded.empty()

    assert spark.sql("SELECT 42 AS value").collect() == [Row(message="stub")]
    assert forwarded.get(timeout=10) == ("command", "sql_command")


