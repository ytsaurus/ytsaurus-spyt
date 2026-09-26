# Native Arrow UDFs over Spark Connect

Build from the SPYT repository root:

```bash
ya make -r examples/native-udf-plugin
```

The build compiles the Connect executable and the sibling Java and C++ targets.
Their outputs are `connect/native_udf_connect`, `java/native-udf-plugin.jar`,
and `native/libspyt_native_udf.so`. Pass their paths with `--jar` and `--library`.
SPYT must provide `columnar-api` and Arrow C Data.

Run with your usual YT credentials (for example, `YT_TOKEN`):

```bash
examples/native-udf-plugin/connect/native_udf_connect \
    --yt-proxy <cluster-proxy> \
    --spyt-version <spyt-version> \
    --jar examples/native-udf-plugin/java/native-udf-plugin.jar \
    --library examples/native-udf-plugin/native/libspyt_native_udf.so \
    --input-table //path/to/scan_table \
    --output-table //path/to/new_result \
    --function increment --column id --delta 10
```

The executable starts a dedicated Spark 4.2.0 Connect driver using Java 25,
uploads the artifacts to a unique directory under `//tmp`, and passes their
Cypress paths through `jars` and `files` to `start_connect_server`. The original
filenames are preserved. Native access is enabled for both driver and executors.
The driver operation ID is printed after launch. The endpoint is printed when executing a job.

To separate server startup from execution, launch without input/output tables:

```bash
examples/native-udf-plugin/connect/native_udf_connect \
    --launch-only --yt-proxy <cluster-proxy> --spyt-version <spyt-version> \
    --jar examples/native-udf-plugin/java/native-udf-plugin.jar \
    --library examples/native-udf-plugin/native/libspyt_native_udf.so
```

Launch-only mode prints the driver operation ID and returns immediately after
submission, without waiting for the Connect endpoint. The execution command waits
for the server to become ready.
Then use that ID for one or more jobs:

```bash
examples/native-udf-plugin/connect/native_udf_connect \
    --yt-proxy <cluster-proxy> --operation-id <operation-id> \
    --input-table //path/to/scan_table --output-table //path/to/new_result \
    --function increment --column id --delta 10
```

For `--launch-only`, `--jar` and `--library` may both be omitted. The server then
starts without preloaded artifacts or Cypress uploads. Supply both artifact paths
when executing later with `--operation-id` to upload them to that session.

The example enables `spark.ytsaurus.columnar.udf.enabled` when launching a server.
An existing server must have been started with that setting enabled.

With `--operation-id`, the example discovers the endpoint from the existing operation
and waits for the server to be ready. No server is launched. `--yt-proxy` is required;
`--spyt-version` is not required. By default the example
uses preloaded artifacts and the library filename `libspyt_native_udf.so`.
`SparkSession.addArtifacts` is called only when `--operation-id`, `--jar`, and
`--library` are explicitly supplied together, with `file=True` for the library.
Supplying only one of `--jar` and `--library` is rejected.

Supported functions and arguments match `examples/python/native_increment/job.py`:

| Function | Columns | Option | Result |
| --- | --- | --- | --- |
| `increment` | One Long column | `--delta` (default 1) | `incremented` |
| `scale` | One Long column | `--factor` (default 2.0) | `scaled` |
| `utf8_length` | One String column | — | `byte_length` |
| `concat` | Two String columns | — | `concatenated` |

Use `--columns first second` for two arguments and `--pool` to choose a scheduler
pool. The output contains the original argument columns and the function result.
When launching a server, use `--driver-memory` (default `4G`), `--executor-memory`
(default `8G`), `--executor-cores` (default `2`), and `--num-executors` (default `2`)
to size its resources, for example:
`--driver-memory 4G --executor-memory 8G --executor-cores 4 --num-executors 3`.
These settings also apply to `--launch-only`; they do not resize an existing
server supplied through `--operation-id`.

An existing output table causes an error. The example always stops its session.
For a combined launch and run, `--stop-driver` (the default) completes the owned
driver operation and removes its Cypress artifacts afterward. Use `--no-stop-driver`
to retain the server. `--launch-only` retains the submitted operation and its uploads;
failures after submission are observed by the later execution command. For a combined
launch and run, startup failures attempt to complete the operation and clean up uploads. Artifacts
are removed only after the driver is confirmed stopped. If completion fails and
the driver is still running or its state cannot be checked, the example retains
the uploads and logs their Cypress directory for later cleanup. Cleanup failures
are reported without replacing the original startup or execution error.

A server supplied via `--operation-id` is never completed by this example, regardless
of `--stop-driver`. Complete it explicitly when finished, for example:

```bash
yt --proxy <cluster-proxy> complete-operation <operation-id>
```

Retained servers keep their Cypress artifacts available for subsequent executors.
The temporary directory expires after seven days of inactivity; remove it manually
after completing a retained server if earlier cleanup is desired.
