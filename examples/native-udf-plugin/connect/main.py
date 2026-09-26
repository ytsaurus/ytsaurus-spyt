import argparse
import logging
import math
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import spyt
from spyt.connect import start_connect_server, wait_for_spark_connect_endpoint
from yt.wrapper import YtClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import call_function, col
from pyspark.sql.types import LongType, StringType


FUNCTIONS = {
    "increment": ("NativeIncrementProvider", (LongType,), "incremented"),
    "scale": ("NativeScaleProvider", (LongType,), "scaled"),
    "utf8_length": ("NativeUtf8LengthProvider", (StringType,), "byte_length"),
    "concat": ("NativeConcatProvider", (StringType, StringType), "concatenated"),
}


def parse_args():
    """Validate launch and execution options before creating remote resources."""
    parser = argparse.ArgumentParser(description="Apply a native Arrow batch UDF to YT columns")
    parser.add_argument("--yt-proxy", required=True, help="YT cluster hosting the Connect driver operation")
    parser.add_argument("--spyt-version", help="Required when launching a new server")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--operation-id", help="Existing Connect driver operation ID")
    mode.add_argument("--launch-only", action="store_true",
                      help="Launch a server and print its operation ID without a job")
    parser.add_argument("--pool", help="YT scheduler pool")
    parser.add_argument("--driver-memory", default="4G", help="Connect driver memory (default: 4G)")
    parser.add_argument("--executor-memory", default="8G", help="Memory per executor (default: 8G)")
    parser.add_argument("--executor-cores", type=int, default=2, help="Cores per executor (default: 2)")
    parser.add_argument("--num-executors", type=int, default=2, help="Number of executors (default: 2)")
    parser.add_argument("--stop-driver", action=argparse.BooleanOptionalAction, default=True,
                        help="Complete the Connect driver operation after success or failure (default: enabled)")
    parser.add_argument("--jar", type=Path,
                        help="Plugin JAR; optional with --launch-only or --operation-id, "
                             "required for launch and execution")
    parser.add_argument("--library", type=Path, help="Local shared library path; must be supplied together with --jar")
    parser.add_argument("--input-table")
    parser.add_argument("--output-table")
    parser.add_argument("--function", choices=FUNCTIONS, default="increment")
    parser.add_argument("--columns", "--column", nargs="+", default=["a"],
                        help="Input column names in argument order (default: a)")
    parser.add_argument("--delta", type=int, help="Increment constant (default: 1)")
    parser.add_argument("--factor", type=float, help="Scale multiplier (default: 2.0)")
    args = parser.parse_args()
    if args.executor_cores <= 0 or args.num_executors <= 0:
        parser.error("--executor-cores and --num-executors must be positive integers")
    if bool(args.jar) != bool(args.library):
        parser.error("--jar and --library must be specified together")
    if not args.operation_id and not args.spyt_version:
        parser.error("Launching a server requires --spyt-version")
    if not args.operation_id and not args.launch_only and not args.jar:
        parser.error("Launching a server and executing a job requires --jar and --library")
    if not args.launch_only and (not args.input_table or not args.output_table):
        parser.error("Executing a job requires --input-table and --output-table")
    _, input_types, _ = FUNCTIONS[args.function]
    if len(args.columns) != len(input_types):
        parser.error(f"{args.function} requires {len(input_types)} input column(s)")
    for artifact in ((args.jar, args.library) if args.jar else ()):
        if not artifact.is_file():
            parser.error(f"Artifact does not exist or is not a file: {artifact}")
    options = {"library": args.library.name if args.library else "libspyt_native_udf.so"}
    if args.delta is not None:
        if args.function != "increment":
            parser.error("--delta is only valid for increment")
        if not -(1 << 63) <= args.delta < (1 << 63):
            parser.error("--delta must fit in a signed 64-bit integer")
        options["delta"] = str(args.delta)
    if args.factor is not None:
        if args.function != "scale":
            parser.error("--factor is only valid for scale")
        if not math.isfinite(args.factor):
            parser.error("--factor must be finite")
        options["factor"] = str(args.factor)
    return args, options


@contextmanager
def cleanup_after(action):
    """Run cleanup on exit while preserving an existing failure and reporting cleanup errors."""
    try:
        yield
    except BaseException:
        try:
            action()
        except BaseException:
            logging.exception("Cleanup failed while handling the original error")
        raise
    else:
        action()


def cleanup_server(client, operation, directory):
    """Complete an owned operation and remove uploads only once the driver is known to be stopped."""
    def remove_artifacts():
        if directory is None:
            return
        if operation is not None:
            try:
                finished = operation.get_state().is_finished()
            except BaseException:
                logging.warning("Retaining Cypress artifacts %s: driver state is unknown", directory)
                raise
            if not finished:
                logging.warning("Retaining Cypress artifacts %s: driver operation %s is still running",
                                directory, operation.id)
                return
        client.remove(directory, recursive=True)

    with cleanup_after(remove_artifacts):
        if operation is not None:
            if not operation.get_state().is_finished():
                client.complete_operation(operation.id)
                operation.wait(check_result=False)
            print(f"Stopped Spark Connect driver operation: {operation.id}", flush=True)


def upload_artifacts(client, args):
    """Upload artifacts with their original filenames to a temporary Cypress directory."""
    directory = f"//tmp/spyt-native-udf-{uuid4()}"
    client.create("map_node", directory, attributes={"expiration_timeout": 7 * 24 * 60 * 60 * 1000})
    try:
        paths = []
        for artifact in (args.jar, args.library):
            path = f"{directory}/{artifact.name}"
            client.create("file", path)
            with artifact.open("rb") as stream:
                client.write_file(path, stream)
            paths.append(path)
        print(f"Cypress artifacts: {directory}", flush=True)
        return directory, paths
    except BaseException:
        with cleanup_after(lambda: client.remove(directory, recursive=True)):
            raise


def launch_server(client, args, paths):
    """Start a Java 25 Connect driver, optionally preloading artifacts for all its sessions."""
    return start_connect_server(
        client, spyt_version=args.spyt_version, spark_version="4.2.0",
        java_home="/opt/jdk25", prefer_ipv6=True, pool=args.pool,
        driver_memory=args.driver_memory, executor_memory=args.executor_memory,
        executor_cores=args.executor_cores, num_executors=args.num_executors,
        jars=paths[:1], files=paths[1:],
        title=f"Native columnar Connect example: {args.function}",
        spark_conf={
            "spark.ytsaurus.columnar.udf.enabled": "true",
            "spark.ytsaurus.network.project": "spark",
            "spark.driver.extraJavaOptions": "--enable-native-access=ALL-UNNAMED -Djava.net.preferIPv6Addresses=true",
            "spark.executor.extraJavaOptions": "--enable-native-access=ALL-UNNAMED -Djava.net.preferIPv6Addresses=true",
        },
    )


def execute_job(endpoint, args, options):
    """Execute the selected function, optionally uploading artifacts to an existing server's session."""
    spark = SparkSession.builder.remote(endpoint).getOrCreate()
    with cleanup_after(spark.stop):
        if args.operation_id and args.jar and args.library:
            spark.addArtifacts(str(args.jar))
            spark.addArtifacts(str(args.library), file=True)
        provider, input_types, output_name = FUNCTIONS[args.function]
        function_name = "native_" + args.function
        spyt.register_columnar_function(
            spark, function_name,
            "tech.ytsaurus.spyt.example." + provider,
            options=options,
        )
        source = spark.read.yt(args.input_table)
        for name, expected_type in zip(args.columns, input_types):
            if name not in source.columns:
                raise ValueError(f"Input column {name!r} does not exist")
            if not isinstance(source.schema[name].dataType, expected_type):
                raise ValueError(f"Column {name!r} must have {expected_type().simpleString()} type")

        values = [col("`" + name.replace("`", "``") + "`") for name in args.columns]
        originals = [value.alias("original" if len(values) == 1 else f"original_{index + 1}")
                     for index, value in enumerate(values)]
        result = source.select(
            *originals,
            call_function(function_name, *values).alias(output_name),
        )
        result.explain(mode="formatted")
        result.write.mode("error").yt(args.output_table)
        print(f"Result written to {args.output_table}", flush=True)


def main():
    """Launch or reuse a Connect server and complete only operations owned by this invocation."""
    args, options = parse_args()
    client = YtClient(proxy=args.yt_proxy)
    if args.operation_id:
        endpoint = wait_for_spark_connect_endpoint(client, args.operation_id, timeout=180)
        endpoint = endpoint if endpoint.startswith("sc://") else f"sc://{endpoint}"
        print(f"Spark Connect endpoint: {endpoint}", flush=True)
        execute_job(endpoint, args, options)
        return

    directory, paths = upload_artifacts(client, args) if args.jar else (None, [])
    operation = None
    retain_operation = False

    def cleanup():
        stop = not retain_operation
        if stop:
            cleanup_server(client, operation, directory)
        elif operation is not None:
            print(f"Spark Connect driver operation left running: {operation.id}", flush=True)

    with cleanup_after(cleanup):
        operation = launch_server(client, args, paths)
        print(f"Spark Connect driver operation: {operation.id}", flush=True)
        if args.launch_only:
            retain_operation = True
            return
        endpoint = wait_for_spark_connect_endpoint(client, operation.id, timeout=180)
        endpoint = endpoint if endpoint.startswith("sc://") else f"sc://{endpoint}"
        retain_operation = not args.stop_driver
        print(f"Spark Connect endpoint: {endpoint}", flush=True)
        execute_job(endpoint, args, options)


if __name__ == "__main__":
    main()
