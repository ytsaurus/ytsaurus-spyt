# YQL UDFs in Spark

This standalone example implements the seven before/after queries using SPYT's
columnar function API, running one selected query per invocation. A Java 25 companion JAR calls a C++ PureCalc bridge through
Arrow C Data. PureCalc loads the supplied YQL UDF `.so` files and evaluates the
original YQL expressions on executor batches. No YQL service is contacted.

A YQL UDF library cannot be passed directly to the providers in
[../native-udf-plugin](../native-udf-plugin): it exports YQL module registration,
not those providers' Arrow functions. This example supplies the missing runtime
adapter. The bridge compiles a projection once per task and reuses it for batches.
The YQL runtime and loaded modules stay in memory for the JVM lifetime because
UDFs resolve process-wide runtime symbols. The plugin reuses one loaded bridge
across Connect session classloaders and artifact paths. Different bridge contents
are rejected before loading; restart the executor JVM to change the bridge.
LLVM and the block optimizer are disabled; Arrow remains the batch transport.
Spark performs the YT scans, distinct, ordering, and limits.

## Build and verify

The Arcadia dependency-policy exception allowing this native target to use
`yql/essentials/public/purecalc` must land separately through the owners of
`build/rules/peerdir_policies/yt.policy`. It is a prerequisite for both the
standalone and parent-directory builds. The exception must target
`yt/spark/yql-udf-plugin/native`.

From this example directory in Arcadia after that prerequisite is applied:

```bash
ya make -r -t . ../../../yql-udf-plugin --add-result=.jar --no-src-links --output build/ya
```

The local target builds the Python executable. The sibling `yt/spark/yql-udf-plugin`
target builds the plugin and PureCalc bridge and runs JUnit tests, including
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
before loading/compiling the UDFs; the executor working directory is irrelevant.

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

Library names are not SQL function names. All supplied modules are loaded into
PureCalc's registry, the equivalent of the original `PRAGMA File` / `PRAGMA Udf`
setup. Missing modules and type errors surface with YQL compilation diagnostics.
The example does not substitute Spark approximations for missing UDFs.

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
  --jar build/ya/yt/spark/yql-udf-plugin/java/yql-udf-plugin.jar \
  --bridge build/ya/yt/spark/yql-udf-plugin/native/libspyt_yql_udf.so \
  --udf-library /path/to/libyabs_enums_udf.so
```

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

The job uploads the JAR, bridge, supplied UDF libraries, and Geo dataset when needed to its Connect session. Providers
resolve filenames through the executor context, including session-specific paths.
It prints the driver operation ID, endpoint, and the selected result table. It closes
its session but leaves the Connect driver operation running, including on query
failure or endpoint startup timeout. Session artifacts and registered functions
must be supplied again when connecting with a new session.

Use `--idle-timeout 30m` to set `spark.ytsaurus.connect.idle.timeout` (default: `10m`).
The server shuts down after that period without requests in progress. To stop it
earlier, use the printed operation ID:

```bash
yt --proxy YOUR_CLUSTER complete-operation OPERATION_ID
```

Use `--pool` to choose a scheduler pool. Resource defaults are
`--driver-memory 4G --executor-memory 8G --executor-cores 2 --num-executors 2`.

## Query mapping and types

| Result label | Projection evaluated by PureCalc |
| --- | --- |
| `Geo::RoundRegionById` | `Geo::RoundRegionById(user_region, 'region').en_name` |
| `ProductsUrls::GetCanonDom` | `ProductsUrls::GetCanonDom(domain)` |
| `UserAgent::Parse` | `UserAgent::Parse(user_agent).BrowserName` |
| `SearchRequest::NormalizeConsistent` | `SearchRequest::NormalizeConsistent(correctedquery)` |
| `YabsEnums::ConvertFlagsToStructOptionsEnum` | Create the callable, apply `options ?? 0ul`, select `.commerce` |
| `H3::FromGeo` | `H3::FromGeo(lon, lat, 9ut)` |
| `Knn::CosineSimilarity` | Decode the first four floats; compare the stored vector to `(1, 0, ..., 0)` of length 256 |

`job.py` contains the complete queries and registrations. It preserves the
original `before` column, displays up to 50 rows per result, and applies
`DISTINCT` to the domain/result pair before ordering and displaying. As in the
original, the other six results have no ordering guarantee: Spark and YQL may
select different samples.

The runner expects the supplied tables' types: integer region IDs, text columns,
UInt64 options, double coordinates, and binary embeddings. Region IDs are cast to
INT in Spark. YQL `String` text results are exposed as Spark STRING through Utf8;
UInt64 options and H3 indexes retain SPYT's UInt64 type. The embedding read uses
a BinaryType schema hint so arbitrary bytes are never decoded as UTF-8. Nulls are
passed to YQL, including the explicit zero fallback for options.

The generic `YqlFunctionProvider` accepts `query`, Arrow JSON `input_schema` and
`output_schema`, `library` (the bridge filename), and newline-separated
`udf_libraries` (UDF filenames), optional `geodata` (dataset filename), and
`deterministic` (`true` by default). Set `deterministic=false` for projections
whose output can change for the same inputs, including nondeterministic UDFs;
this declaration controls Spark optimization and does not inspect the YQL query.
The runner requires `--geodata` only for `--query region`.
Projections must preserve row count and order. Scalar queries return the declared output field; struct queries
return its children as separate scalar fields, which the bridge packs into the
result struct. Names, types, and nullability must match the YQL result exactly.
In particular, commerce is non-nullable because its input is coalesced to zero.
Filtering, grouping, joins, ordering, and limits belong in Spark. This example supports primitive Arrow types
and a top-level result struct. For KNN, PureCalc returns four decoded dimensions,
the preview length, and similarity; Spark reconstructs the preview array. This
avoids passing lists through PureCalc's scalar-only Arrow output interface.
It is a batch transport adapter: individual
YQL UDF implementations may still evaluate one row at a time.
