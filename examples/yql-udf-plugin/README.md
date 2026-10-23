# YQL UDFs in Spark

This standalone example implements the seven before/after queries using SPYT's
columnar function API, running one selected query per invocation. A Java 25
companion JAR calls a C++ bridge through Arrow C Data. The bridge loads the supplied
YQL UDF `.so` files and invokes their callables directly through the YQL runtime.
`job.py` contains Spark SQL queries and registers UDF names and Arrow schemas.
There are no YQL query strings or generated YQL projections. No YQL service is contacted.

A YQL UDF library cannot be passed directly to the providers in
[../native-udf-plugin](../native-udf-plugin): it exports YQL module registration,
not those providers' Arrow functions. This example supplies the missing runtime
adapter. The bridge binds a callable once per task and reuses it for batches.
The YQL runtime and loaded modules stay in memory for the JVM lifetime because
UDFs resolve process-wide runtime symbols. The plugin reuses one loaded bridge
across Connect session classloaders and artifact paths. Different bridge contents
are rejected before loading; restart the executor JVM to change the bridge.
The bridge links directly to the MiniKQL runtime and the exception-based UDF service
policy. Arrow transports batches, and the bridge invokes
the scalar UDF once per row. Spark performs the YT scans, distinct, ordering and limits.

## Build and verify

The native target depends on MiniKQL, its computation and runtime settings libraries,
builtins without LLVM, the UDF service exception policy, and the PostgreSQL runtime
stub. The bridge supports YQL data types; PostgreSQL types are not supported. These
dependencies have a plugin-specific exception in `build/rules/peerdir_policies/yt.policy`.

From this example directory in a Linux Arcadia checkout:

```bash
ya make -r -t . ../../../yql-udf-plugin --add-result=.jar --no-src-links --output build/ya
```

From macOS, add `--dist --target-platform=default-linux-x86_64` to build and test
on Linux remotely, and `-E` to download the resulting artifacts.

The local target builds the Python executable. The sibling `yt/spark/yql-udf-plugin`
target builds the plugin and native bridge and runs JUnit tests, including
dynamically loaded H3 and Knn UDFs. Artifacts under `build/ya` are:

- `yt/spark/yql-udf-plugin/java/yql-udf-plugin.jar`
- `yt/spark/yql-udf-plugin/native/libspyt_yql_udf.so`
- `yt/spark/spark-over-yt/examples/yql-udf-plugin/yql_udf_connect`

SPYT supplies `columnar-api` and Arrow C Data; the example is excluded from the
SPYT distributive and Gradle build. Use Spark 4.2.0, a SPYT build with columnar
functions, and Java 25 on the driver and executors. The runner configures the
new Connect server with columnar functions and native access enabled on both
driver and executors. This bridge targets Linux. The native
libraries must match the executors' OS, architecture, and the bridge's YQL ABI.

## Supply the UDF libraries

The custom `libyabs_enums_udf.so` supplies `YabsEnums` only. The other modules in
the original queries are installed in the YQL service; Spark needs their shared
libraries only for the query you select with `--query`. Supply its library with
`--udf-library`; repeat the argument if that query needs additional modules.
You do not need libraries or input tables for unselected queries. Library filenames
may differ from those below; the selected YQL module must be present in the supplied files.
For `--query region`, also pass the Geo dataset with `--geodata /path/to/geodata6.bin` (geodata5.bin is
also supported). Use the same dataset version as the original YQL query when
comparing results. The job uploads it alongside the libraries to its Spark Connect session. The provider
resolves its executor-local path and the bridge sets `GEOBASE_YQL_FIXED_PATH`
before loading the UDFs; the executor working directory is irrelevant.

Geo caches its lookup process-wide. Identical datasets uploaded in different
sessions are accepted; selecting different content requires restarting the
executor JVM. The bridge checks a cached file fingerprint and fails explicitly
on a mismatch. Distributed artifacts must remain immutable. Distribute any
additional native dependencies required by your UDF libraries separately.

| `--query` | YQL module | Arcadia build target, or externally supplied library |
| --- | --- | --- |
| `region` | `Geo` | `yql/udfs/common/geo` (`libgeo_udf.so`) |
| `domain` | `ProductsUrls` | `yql/udfs/products/canonizer` (`libproducts_urls_udf.so`) |
| `browser` | `UserAgent` | `yql/udfs/common/user_agent` (`libuser_agent_udf.so`) |
| `normalize` | `SearchRequest` | `yql/udfs/quality/search_request` (`libsearch_request_udf.so`) |
| `commerce` | `YabsEnums` | The supplied `libyabs_enums_udf.so` |
| `h3` | `H3` | `yql/udfs/common/h3` (`libh3_udf.so`) |
| `knn` | `Knn` | `contrib/ydb/library/yql/udfs/common/knn` (`libknn_udf.so`) |

Download the supplied file using your configured YT credentials:

```bash
yt --proxy YOUR_CLUSTER read-file \
  //path/to/udfs/libyabs_enums_udf.so \
  > libyabs_enums_udf.so
```

Library names are not SQL function names. The bridge loads all supplied modules
into the YQL function registry. Missing modules and incompatible signatures fail
when the task binds the callable.

## Run through Spark Connect

Run with your usual YT credentials and a Python environment containing matching
PySpark, SPYT, and the YT Python client. The runner starts a dedicated Spark 4.2.0
Connect server using Java 25 on the specified cluster and waits for its endpoint:

```bash
python job.py \
  --yt-proxy YOUR_CLUSTER \
  --spyt-version YOUR_SPYT_VERSION \
  --query commerce \
  --input-root //path/to/sample-tables \
  --udf-library /path/to/libyabs_enums_udf.so
```

The selected SPYT version must provide the `yql-udf-plugin` configuration entry.
The entry contains the plugin root path; its `conf` document supplies `file_paths`
and `spark_conf`. The runner passes the plugin name to `start_connect_server`, which
loads the files and Spark defaults, including the plugin JAR classpaths and native access.
Plugin roots are read from the global registry and overridden by the selected SPYT
version configuration. Global registration does not enable a plugin: callers must
request it explicitly. Use a version-specific entry when that SPYT version needs a
different plugin build. The YQL UDF plugin requires Spark 4.2.0 or newer and Java 25,
regardless of where its root is registered.
Plugins are applied in the supplied order. Later plugin scalar defaults take precedence,
while JVM options and classpath entries retain plugin order; reordering conflicting
plugins changes the effective configuration and prevents reusing a differently configured server.
Plugin JVM options and classpaths are appended to existing values, using spaces and
colons respectively. Other explicit Spark settings override plugin defaults. Standalone clusters load the same
configuration when `enable_yql_udf` is enabled.
Build and publish the JAR and native bridge together. This provider uses the
`function` option, replacing the previous `query` option, and requires the rebuilt plugin.

The built `build/ya/yt/spark/spark-over-yt/examples/yql-udf-plugin/yql_udf_connect` executable
accepts the same arguments as `python job.py`. Its build target resides alongside `job.py`.

To run H3 instead, use `--query h3 --udf-library /path/to/libh3_udf.so`.
For Geo, use `--query region --udf-library /path/to/libgeo_udf.so` and
`--geodata /path/to/geodata6.bin`. Other queries do not require Geo data.

Use `--runs 5 --delay-seconds 10` to execute the selected query five times,
waiting ten seconds between completed runs. Defaults are one run and no delay;
fractional seconds are supported. All runs reuse the same Connect session,
uploaded artifacts, and registered function. Each run displays up to 50 rows.
Each run prints its elapsed wall time in seconds, including result display but
excluding session setup, artifact upload, and the delay between runs.
There is no delay before the first run or after the last; a failed run stops the loop.
Set `--idle-timeout` longer than the delay so the server stays available between runs.

The job uploads the supplied UDF libraries and Geo dataset when needed to its
Connect session. Providers resolve filenames through the executor context,
including session-specific paths.
It prints the driver operation ID, endpoint, and the selected result table. It closes
its session but leaves the Connect driver operation running, including on query
failure or endpoint startup timeout. Session artifacts and registered functions
must be supplied again when connecting with a new session.

Use `--idle-timeout 30m` to set `spark.ytsaurus.connect.idle.timeout` (default: `10m`).
The server shuts down after that period without requests in progress. To stop it
earlier, use the printed operation ID:

```bash
yt --proxy YOUR_CLUSTER complete-op OPERATION_ID
```

Use `--pool` to choose a scheduler pool. Resource defaults are
`--driver-memory 4G --executor-memory 8G --executor-cores 2 --num-executors 2`.

## Query mapping and types

| Result label | Spark SQL call or result |
| --- | --- |
| `Geo::RoundRegionById` | `yql_region(CAST(user_region AS INT), 'region').en_name` |
| `ProductsUrls::GetCanonDom` | `yql_domain(domain)` |
| `UserAgent::Parse` | `yql_browser(user_agent).BrowserName` |
| `SearchRequest::NormalizeConsistent` | `yql_normalize(correctedquery)` |
| `YabsEnums::ConvertFlagsToStructOptionsEnum` | `yql_commerce(options).commerce`, with null options defaulted to zero |
| `H3::FromGeo` | `yql_h3(lon, lat, 9)` |
| `Knn::CosineSimilarity` | `yql_knn_decode`, `yql_knn_encode` and `yql_knn_similarity`; a 256-dimensional unit vector |

The Spark SQL queries preserve the original `before` column and apply `DISTINCT` to the domain/result
pair before ordering. The runner displays up to 50 rows per result. As in the
original, the other six results have no ordering guarantee: Spark and YQL may
select different samples.

The runner expects the supplied tables' types: integer region IDs, text columns,
UInt64 options, double coordinates, and binary embeddings. Region IDs are cast to
INT in Spark. YQL `String` text results are exposed as Spark STRING with UTF-8 validation;
UInt64 options and H3 indexes retain SPYT's UInt64 type. The embedding read uses
a BinaryType schema hint so arbitrary bytes are never decoded as UTF-8. Nulls are
passed to optional YQL arguments; nulls in AutoMap arguments propagate to a null
result. The options argument declares a zero default through Arrow field metadata.

The generic `YqlFunctionProvider` accepts `function` (a name such as `H3::FromGeo`), Arrow JSON `input_schema` and
`output_schema`, optional `library` (the bridge filename, defaulting to
`libspyt_yql_udf.so`), and newline-separated
`udf_libraries` (UDF filenames), optional `geodata` (dataset filename), and
`deterministic` (`true` by default). Set `deterministic=false` for UDFs
whose output can change for the same inputs, including nondeterministic UDFs;
this declaration controls Spark optimization. `callable=true` initializes a
zero-argument UDF factory once per task and invokes the returned callable on each
row, as required by the commerce example. Non-void YQL RunConfig is not supported.
The runner requires `--geodata` only for `--query region`.
The provider supports primitive and list arguments, and primitive, list and struct
results. A declared result struct may select members by name, so the Geo and
UserAgent registrations expose only the fields used in Spark SQL. Numeric
arguments and results use checked Arrow casts; for example, the INT resolution
argument becomes the Uint8 expected by H3. String and Utf8 use Arrow binary and
UTF-8 arrays respectively. The supplied schemas also provide argument types to
type-aware UDFs such as Knn. Omitted trailing optional arguments are passed empty.

An input field can set `metadata: [{"key": "yql.default", "value": "0"}]` to replace nulls with a
typed scalar before invoking the UDF. This supplies the commerce zero fallback
while preserving UInt64 values. Defaults use Arrow's scalar parser and must
match the UDF argument type. Result nullability is enforced, including nested
fields and list items. Filtering, grouping, joins, ordering and limits belong in Spark.
