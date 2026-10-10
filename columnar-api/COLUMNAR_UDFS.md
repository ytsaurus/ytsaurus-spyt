# Columnar function plugins

SPYT runs Java plugins on Arrow batches. Plugins may compute in Java or call native
libraries through their own ABI. This API targets Spark 4.2.0, Scala 2.13,
and Arrow 19.0.0.

## Dependencies and configuration

`columnar-api` exposes the Java 17 provider, descriptor, evaluator, and executor
context interfaces (API version `1`). `data-source-extended` implements registration,
planning, conversion, and resource management. The SPYT distributive includes the
API JAR and Arrow C Data; Spark supplies the remaining Arrow libraries.

Compile plugins against the target runtime's Arrow version. Do not bundle Arrow
or SPYT API classes. Configure the JDK and JVM options required by the plugin on
both driver and executors. Java 25 FFM plugins require Java 25 and
`--enable-native-access=ALL-UNNAMED` on both.

Before creating the Spark session, configure:

- `spark.ytsaurus.columnar.udf.enabled=true` (default: `false`).
- `spark.sql.extensions=tech.ytsaurus.spyt.format.YtSparkExtensions`.

Enabling the flag later does not install the registration parser. When disabled,
SPYT leaves the parser unchanged and skips the columnar planning rule.

## Provider lifecycle

Implement `tech.ytsaurus.spyt.format.columnar.ColumnarFunctionProvider` with a
public no-argument constructor:

- `describe(options)` returns `ColumnarFunctionDescriptor(inputSchema,
  outputSchema, deterministic)`. Input fields correspond to positional SQL
  arguments. Output must contain exactly one field; use a struct for multiple
  values. Schemas must not contain Arrow dictionaries.
- `create(context, options)` returns a partition-local `ColumnarEvaluator`.
  Initialize native resources here. Evaluators are created lazily; empty
  partitions do not create them.

Options are immutable string-to-string maps. The constructor and `describe` run
on the driver and must not depend on executor-local files or native initialization.
SPYT verifies that executor schemas and determinism match registration metadata
and rejects unsupported API versions.

The context exposes `allocator()` and `resolveArtifact(filename)`. The allocator
belongs to SPYT. Artifact names must be bare filenames; the resolver returns
executor-local paths, preferring session artifacts over shared Spark files.

## Batch ownership

`ColumnarEvaluator.evaluate(input)` receives a borrowed, read-only
`VectorSchemaRoot`. Do not mutate or close it or its vectors and buffers. SPYT
releases the input view after the call. Compatible Arrow buffers are shared within
the same allocator root; other inputs are copied or converted to preserve lifetime
independence from upstream batches. Normal YT scans use a separate allocator root,
so their columns are copied. Flat Arrow columns use bulk copies; compatible
arguments and pass-through columns reuse one conversion per source column per batch.

Return a distinct, owned root with the registered schema and unchanged row count.
SPYT owns the result after return. Outputs referencing input buffers must retain
them independently. On failure, release partial outputs before throwing.

SPYT checks output schemas and top-level vector row counts. Sources and providers
are trusted to supply valid Arrow structure and honor declared nullability,
including nested fields; SPYT does not scan values to validate these contracts.

Results remain valid until closed, the next batch is requested, or the task ends.
Probing iterator availability does not release them. Failure and task completion,
including early `LIMIT`, close outstanding results, evaluators, and the allocator.
Evaluator `close()` must release plugin-owned resources without closing the
context allocator.

## Registration and execution

Distribute plugin JARs and dependencies before registration. Use `--jars` and
`--files`/`SparkContext.addFile` for classic Spark, or `spark.addArtifact(jarPath)`
and `spark.addArtifact(libraryPath, file=True)` for Spark Connect. Resolve files
through the executor context instead of passing driver-local absolute paths.

Register a session-local function with:

```sql
REGISTER COLUMNAR FUNCTION function_name AS 'package.ProviderClass'
OPTIONS '{"key":"value"}';
```

`OPTIONS` is optional. Python callers can use
`spyt.register_columnar_function(spark, name, provider, options)`.
Providers are loaded through the session's artifact classloader.

Calls support pass-through columns, multiple outputs, computed arguments, nested
calls, and surrounding Spark expressions in projections. Nested calls execute in
dependency order; nondeterministic occurrences have independent evaluators.
Evaluation is eager: surrounding conditionals do not short-circuit plugin calls.

Columnar functions are supported only in top-level projections

There is no scalar fallback. Functions that change the number of rows are unsupported.

## Build and verification

Build with `./gradlew jar` or `./gradlew clean spytDistributive`.

Engine coverage is in `YtColumnarFunctionExecTest` and
`ColumnarFunctionProjectionTest` in `data-source-extended`. These tests use Java
providers without native libraries; YT scan tests require a local YT cluster.
