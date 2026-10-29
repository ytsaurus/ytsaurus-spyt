import argparse
import logging
import math
import time
from contextlib import contextmanager
from pathlib import Path

import spyt
from spyt.connect import start_connect_server, wait_for_spark_connect_endpoint
from yt.wrapper import YtClient
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType

QUERIES = {
    "region": ("surplus_inputs", "Geo::RoundRegionById", """
        SELECT user_region AS before, yql_region(CAST(user_region AS INT), 'region').en_name AS after
        FROM Input
    """),
    "domain": ("product_domains", "ProductsUrls::GetCanonDom", """
        SELECT DISTINCT domain AS before, yql_domain(domain) AS after
        FROM Input
        ORDER BY before
    """),
    "browser": ("session_inputs", "UserAgent::Parse", """
        SELECT user_agent AS before, yql_browser(user_agent).BrowserName AS after
        FROM Input
    """),
    "normalize": ("surplus_inputs", "SearchRequest::NormalizeConsistent", """
        SELECT correctedquery AS before, yql_normalize(correctedquery) AS after
        FROM Input
    """),
    "commerce": ("product_money_options", "YabsEnums::ConvertFlagsToStructOptionsEnum", """
        SELECT options AS before, yql_commerce(options).commerce AS after
        FROM Input
    """),
    "h3": ("region_centers", "H3::FromGeo", """
        SELECT named_struct('lon', lon, 'lat', lat) AS before, yql_h3(lon, lat, 9) AS after
        FROM Input
    """),
    "knn": ("knn_float_vectors", "Knn::CosineSimilarity", """
        SELECT slice(yql_knn_decode(binary_emb_float), 1, 4) AS before,
               yql_knn_similarity(binary_emb_float, yql_knn_encode(
                   transform(sequence(0, 255), i -> CAST(IF(i = 0, 1, 0) AS FLOAT)))) AS after
        FROM Input
    """),
}


def field(name, kind, nullable=True, children=None, metadata=None):
    arrow_types = {
        "int": {"name": "int", "bitWidth": 32, "isSigned": True},
        "uint64": {"name": "int", "bitWidth": 64, "isSigned": False},
        "double": {"name": "floatingpoint", "precision": "DOUBLE"},
        "float": {"name": "floatingpoint", "precision": "SINGLE"},
        "string": {"name": "utf8"},
        "binary": {"name": "binary"},
        "bool": {"name": "bool"},
        "struct": {"name": "struct"},
        "list": {"name": "list"},
    }
    result = {"name": name, "nullable": nullable, "type": arrow_types[kind], "children": children or []}
    if metadata:
        result["metadata"] = [{"key": key, "value": value} for key, value in metadata.items()]
    return result


def register(spark, args, name, function, inputs, output, returns_callable=False):
    from spyt_yql_udf import register_yql_function

    register_yql_function(
        spark, name, function, inputs, output, [path.name for path in args.udf_library],
        geodata=args.geodata.name if args.query == "region" else None, returns_callable=returns_callable)


def register_function(spark, args):
    definitions = {
        "region": [
            ("yql_region", "Geo::RoundRegionById", [field("id", "int"), field("level", "string")],
             field("value", "struct", children=[field("en_name", "string")]))],
        "domain": [
            ("yql_domain", "ProductsUrls::GetCanonDom", [field("domain", "string")], field("value", "string"))],
        "browser": [
            ("yql_browser", "UserAgent::Parse", [field("user_agent", "string")],
             field("value", "struct", children=[field("BrowserName", "string")]))],
        "normalize": [
            ("yql_normalize", "SearchRequest::NormalizeConsistent", [field("query", "string")],
             field("value", "string"))],
        "commerce": [
            ("yql_commerce", "YabsEnums::ConvertFlagsToStructOptionsEnum",
             [field("options", "uint64", metadata={"yql.default": "0"})],
             field("value", "struct", False, children=[field("commerce", "bool", False)]), True)],
        "h3": [
            ("yql_h3", "H3::FromGeo", [field("lon", "double"), field("lat", "double"), field("resolution", "int")],
             field("value", "uint64"))],
        "knn": [
            ("yql_knn_decode", "Knn::FloatFromBinaryString", [field("vector", "binary")],
             field("value", "list", children=[field("item", "float", False)])),
            ("yql_knn_encode", "Knn::ToBinaryStringFloat",
             [field("vector", "list", children=[field("item", "float", False)])], field("value", "binary")),
            ("yql_knn_similarity", "Knn::CosineSimilarity", [field("left", "binary"), field("right", "binary")],
             field("value", "float"))],
    }
    for definition in definitions[args.query]:
        register(spark, args, *definition)


def run_query(spark, args):
    table, label, query = QUERIES[args.query]
    reader = spark.read
    if args.query == "knn":
        # Preserve arbitrary YQL String bytes, including invalid UTF-8 and zero bytes.
        reader = reader.schema_hint({"binary_emb_float": BinaryType()})
    source = reader.yt(f"{args.input_root.rstrip('/')}/{table}")
    source.createOrReplaceTempView("Input")
    result = spark.sql(query)
    print(label, flush=True)
    result.show(50, truncate=False)


def native_artifacts(args):
    artifacts = list(args.udf_library)
    if args.query == "region":
        artifacts.append(args.geodata)
    return artifacts


@contextmanager
def cleanup_after(action):
    """Report cleanup failures without replacing an existing startup or query error."""
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


def launch_server(client, args):
    return start_connect_server(
        client, spyt_version=args.spyt_version, spark_version="4.2.0",
        java_home="/opt/jdk25", prefer_ipv6=False, pool=args.pool, reuse_existing=False,
        plugins=["yql-udf-plugin"],
        driver_memory=args.driver_memory, executor_memory=args.executor_memory,
        executor_cores=args.executor_cores, num_executors=args.num_executors,
        title="YQL UDF Spark Connect example",
        spark_conf={
            "spark.ytsaurus.connect.idle.timeout": args.idle_timeout,
        },
    )


def execute_job(endpoint, args):
    spark = SparkSession.builder.remote(endpoint).getOrCreate()
    with cleanup_after(spark.stop):
        for path in native_artifacts(args):
            spark.addArtifact(str(path), file=True)
        register_function(spark, args)
        for run in range(args.runs):
            if run > 0:
                time.sleep(args.delay_seconds)
            print(f"Query run {run + 1}/{args.runs}", flush=True)
            started = time.perf_counter()
            try:
                run_query(spark, args)
            finally:
                elapsed = time.perf_counter() - started
                print(f"Query run {run + 1}/{args.runs} wall time: {elapsed:.3f} seconds", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Run one YQL UDF example through Spark Connect in SPYT")
    parser.add_argument("--query", choices=QUERIES, required=True, help="Example query to execute")
    parser.add_argument("--runs", type=int, default=1, help="Number of query executions (default: 1)")
    parser.add_argument("--delay-seconds", type=float, default=0,
                        help="Delay after each completed run before the next, in seconds (default: 0)")
    parser.add_argument("--input-root", required=True, help="Cypress directory containing the selected sample table")
    parser.add_argument("--yt-proxy", required=True, help="YT cluster hosting the tables and Connect driver")
    parser.add_argument("--spyt-version", required=True, help="SPYT version for the new Connect server")
    parser.add_argument("--pool", help="YT scheduler pool")
    parser.add_argument("--idle-timeout", default="10m",
                        help="Spark Connect server idle timeout, e.g. 30m or 1h (default: 10m)")
    parser.add_argument("--driver-memory", default="4G")
    parser.add_argument("--executor-memory", default="8G")
    parser.add_argument("--executor-cores", type=int, default=2)
    parser.add_argument("--num-executors", type=int, default=2)
    parser.add_argument("--geodata", type=Path,
                        help="Required only for --query region: geodata5.bin or geodata6.bin")
    parser.add_argument("--udf-library", type=Path, action="append", required=True,
                        help="Local YQL UDF .so for the selected query; repeat if it needs additional modules")
    args = parser.parse_args()
    if args.runs <= 0:
        parser.error("--runs must be a positive integer")
    if not math.isfinite(args.delay_seconds) or args.delay_seconds < 0:
        parser.error("--delay-seconds must be a finite nonnegative number")
    if args.executor_cores <= 0 or args.num_executors <= 0:
        parser.error("--executor-cores and --num-executors must be positive integers")
    if args.query == "region" and args.geodata is None:
        parser.error("--query region requires --geodata")
    if args.query != "region" and args.geodata is not None:
        parser.error("--geodata is only used with --query region")
    artifacts = native_artifacts(args)
    for path in artifacts:
        if not path.is_file():
            parser.error(f"Artifact does not exist: {path}")
    if len({path.name for path in artifacts}) != len(artifacts):
        parser.error("Artifact basenames must be unique")
    client = YtClient(proxy=args.yt_proxy)
    spyt.load_plugin(client, "yql-udf-plugin", spyt_version=args.spyt_version)
    operation = launch_server(client, args)
    print(f"Spark Connect driver operation: {operation.id}", flush=True)
    endpoint = wait_for_spark_connect_endpoint(client, operation.id, timeout=180)
    endpoint = endpoint if endpoint.startswith("sc://") else f"sc://{endpoint}"
    print(f"Spark Connect endpoint: {endpoint}", flush=True)
    execute_job(endpoint, args)


if __name__ == "__main__":
    main()
