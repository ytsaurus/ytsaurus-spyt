import argparse
import json
import logging
import math
import time
from contextlib import contextmanager
from pathlib import Path

import spyt
from spyt.connect import start_connect_server, wait_for_spark_connect_endpoint
from yt.wrapper import YtClient
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BinaryType


PROVIDER = "tech.ytsaurus.spyt.yql.YqlFunctionProvider"
QUERIES = {
    "region": ("surplus_inputs", "Geo::RoundRegionById", "user_region"),
    "domain": ("product_domains", "ProductsUrls::GetCanonDom", "domain"),
    "browser": ("session_inputs", "UserAgent::Parse", "user_agent"),
    "normalize": ("surplus_inputs", "SearchRequest::NormalizeConsistent", "correctedquery"),
    "commerce": ("product_money_options", "YabsEnums::ConvertFlagsToStructOptionsEnum", "options"),
    "h3": ("region_centers", "H3::FromGeo", None),
    "knn": ("knn_float_vectors", "Knn::CosineSimilarity", "binary_emb_float"),
}


def field(name, kind, nullable=True):
    arrow_types = {
        "int": {"name": "int", "bitWidth": 32, "isSigned": True},
        "uint64": {"name": "int", "bitWidth": 64, "isSigned": False},
        "double": {"name": "floatingpoint", "precision": "DOUBLE"},
        "float": {"name": "floatingpoint", "precision": "SINGLE"},
        "string": {"name": "utf8"},
        "binary": {"name": "binary"},
        "bool": {"name": "bool"},
        "knn": {"name": "struct"},
    }
    children = ([field("after", "float"), *[field(f"d{i}", "float") for i in range(4)], field("size", "int")]
                if kind == "knn" else [])
    return {"name": name, "nullable": nullable, "type": arrow_types[kind], "children": children}


def register(spark, args, name, inputs, output, query):
    options = {
        "library": args.bridge.name,
        "udf_libraries": "\n".join(path.name for path in args.udf_library),
        "input_schema": json.dumps({"fields": [field(key, kind) for key, kind in inputs]}),
        "output_schema": json.dumps({"fields": [output]}),
        "query": query,
    }
    if args.query == "region":
        options["geodata"] = args.geodata.name
    spyt.register_columnar_function(spark, name, PROVIDER, options=options)


def register_function(spark, args):
    functions = [
        ("yql_region", [("user_region", "int")], field("value", "string"),
         "SELECT CAST(Geo::RoundRegionById(user_region, 'region').en_name AS Utf8) AS value FROM Input"),
        ("yql_domain", [("domain", "string")], field("value", "string"),
         "SELECT CAST(ProductsUrls::GetCanonDom(CAST(domain AS String)) AS Utf8) AS value FROM Input"),
        ("yql_browser", [("user_agent", "string")], field("value", "string"),
         "SELECT CAST(UserAgent::Parse(CAST(user_agent AS String)).BrowserName AS Utf8) AS value FROM Input"),
        ("yql_normalize", [("correctedquery", "string")], field("value", "string"),
         "SELECT CAST(SearchRequest::NormalizeConsistent(correctedquery) AS Utf8) AS value FROM Input"),
        ("yql_commerce", [("options", "uint64")], field("value", "bool", False),
         "$flags = YabsEnums::ConvertFlagsToStructOptionsEnum(); "
         "SELECT $flags(options ?? 0ul).commerce AS value FROM Input"),
        ("yql_h3", [("lon", "double"), ("lat", "double")], field("value", "uint64"),
         "SELECT H3::FromGeo(lon, lat, 9ut) AS value FROM Input"),
        ("yql_knn", [("binary_emb_float", "binary")], field("value", "knn"),
         "$target = Knn::ToBinaryStringFloat(ListMap(ListFromRange(0ul, 256ul), "
         "($i) -> { RETURN IF($i == 0ul, 1.0f, 0.0f); })); "
         "$rows = SELECT binary_emb_float, "
         "ListTake(Knn::FloatFromBinaryString(binary_emb_float), 4) AS head FROM Input; "
         "SELECT Knn::CosineSimilarity(binary_emb_float, $target) AS after, "
         "head[0] AS d0, head[1] AS d1, head[2] AS d2, head[3] AS d3, "
         "CAST(ListLength(head) AS Int32) AS size FROM $rows"),
    ]
    definition = next(definition for definition in functions if definition[0] == f"yql_{args.query}")
    register(spark, args, *definition)


def run_query(spark, args):
    table, label, column = QUERIES[args.query]
    reader = spark.read
    if args.query == "knn":
        # Preserve arbitrary YQL String bytes, including invalid UTF-8 and zero bytes.
        reader = reader.schema_hint({"binary_emb_float": BinaryType()})
    source = reader.yt(f"{args.input_root.rstrip('/')}/{table}")
    function = f"yql_{args.query}"
    if args.query == "knn":
        values = source.select(F.call_function(function, F.col(column)).alias("knn"))
        # PureCalc returns scalar fields; Spark reconstructs the preview list from them.
        preview = F.slice(F.array(*[F.col(f"knn.d{i}") for i in range(4)]), 1, F.col("knn.size"))
        result = values.select(preview.alias("before"), F.col("knn.after").alias("after"))
    elif args.query == "h3":
        result = source.select(
            F.struct("lon", "lat").alias("before"),
            F.call_function(function, F.col("lon"), F.col("lat")).alias("after"))
    else:
        value = F.col(column)
        argument = value.cast("int") if args.query == "region" else value
        result = source.select(value.alias("before"), F.call_function(function, argument).alias("after"))
        if args.query == "domain":
            result = result.distinct().orderBy("before")
    print(label, flush=True)
    result.show(50, truncate=False)


def native_artifacts(args):
    artifacts = [args.bridge, *args.udf_library]
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
        java_home="/opt/jdk25", prefer_ipv6=True, pool=args.pool, reuse_existing=False,
        driver_memory=args.driver_memory, executor_memory=args.executor_memory,
        executor_cores=args.executor_cores, num_executors=args.num_executors,
        title="YQL UDF Spark Connect example",
        spark_conf={
            "spark.ytsaurus.connect.idle.timeout": args.idle_timeout,
            "spark.ytsaurus.columnar.udf.enabled": "true",
            "spark.ytsaurus.network.project": "spark",
            "spark.driver.extraJavaOptions": "--enable-native-access=ALL-UNNAMED -Djava.net.preferIPv6Addresses=true",
            "spark.executor.extraJavaOptions": "--enable-native-access=ALL-UNNAMED -Djava.net.preferIPv6Addresses=true",
        },
    )


def execute_job(endpoint, args):
    spark = SparkSession.builder.remote(endpoint).getOrCreate()
    with cleanup_after(spark.stop):
        spark.addArtifact(str(args.jar))
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
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--bridge", type=Path, required=True)
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
    artifacts = [args.jar, *native_artifacts(args)]
    for path in artifacts:
        if not path.is_file():
            parser.error(f"Artifact does not exist: {path}")
    if len({path.name for path in artifacts}) != len(artifacts):
        parser.error("Artifact basenames must be unique")
    client = YtClient(proxy=args.yt_proxy)
    operation = launch_server(client, args)
    print(f"Spark Connect driver operation: {operation.id}", flush=True)
    endpoint = wait_for_spark_connect_endpoint(client, operation.id, timeout=180)
    endpoint = endpoint if endpoint.startswith("sc://") else f"sc://{endpoint}"
    print(f"Spark Connect endpoint: {endpoint}", flush=True)
    execute_job(endpoint, args)


if __name__ == "__main__":
    main()
