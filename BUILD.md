## Building YTsaurus SPYT from sources

### Build Requirements

- JDK 17 or higher (OpenJDK is preferred)
- Gradle (a Gradle wrapper is included in the repository)
- Python 3.9+
- pip3
- wheel

### How to Build

To build YTsaurus SPYT distributive from sources the assemble Gradle task should be used:

```bash
./gradlew assemble
```

This will create a SPYT assembly archive and Python wheel package inside the `spyt-package/build/output` directory. After that you can install the wheel package into your Python environment and deploy the assembly to the cluster.

Installing Python whl package:

```bash
pip install $(ls spyt-package/build/wheel/dist/*.whl)
```

Deploying SPYT distributive to YTsaurus cluster:
```bash
./tools/release/deploy_snapshot.sh --proxy $YT_PROXY
```

### Running tests

#### Prerequisites

Almost all unit tests are using local YTsaurus cluster in Docker as described here: https://ytsaurus.tech/docs/overview/try-yt#using-docker. The local cluster must be started with at least one RPC proxy and two nodes. This can be done with parameters `--rpc-proxy-count 1 --node-count 2`

It is recommended to build a custom YTsaurus image with a SPYT-friendly environment: navigate to the directory `e2e-test/yt_local` and run `build.sh`. This will build an image and tag it as `spyt-testing`.

An example command for starting the local cluster with one RPC proxy and two nodes:

```bash
run_local_cluster.sh --rpc-proxy-count 1 --node-count 2 --yt-version spyt-testing --yt-skip-pull true
```

After finishing working with unit tests you can stop the local YTsaurus cluster with the following command:

```bash
run_local_cluster.sh --stop
```

#### Running scala unit tests

To launch scala unit tests the following command should be used from the root of the project (assuming that the local YTsaurus cluster is launched):

```bash
./gradlew test
```

To run a specific test class a `--tests` parameter can be used:

```bash
./gradlew <project module>:test --tests "<full test class name>"
```

An example:

```bash
./gradlew data-source-base:test --tests "tech.ytsaurus.spyt.format.YtDynamicTableWriterTest"
```

#### Running scala unit tests over different Spark version

By default unit tests are run over compile Spark version which is defined [here](gradle/libs.versions.toml#L5). To use different Spark version in runtime for unit tests a `-PtestSparkVersion=x.x.x` parameter should be specified for the `./gradlew test` command. For example to test SPYT over Spark 3.2.2 the following command should be used: `./gradlew test -PtestSparkVersion=3.2.2`.

#### Running python integration tests

Prerequisites for running Python unit tests:
- Python 3.9, Python 3.11, Python 3.12, or Python 3.13
- tox >= 4
- wheel
- build

Python tests are located in the `e2e-test` directory.

Before starting the tests, it's required to build a YTsaurus image with a SPYT-friendly environment: visit the directory `e2e-test/yt_local` and run `build.sh`. This can be done once on a testing machine.

Tests are run using [tox](https://tox.wiki) python environment management tool, but you can use `run-tests.sh` script for convenience.

`run-tests.sh` rebuilds the project by default, but if you have already built the project you can skip build stage by passing `--no-rebuild` flag to the script. Also if you don't need SPYT artifacts in cypress then option `--no-deploy` may be used.

To run tests using specific Python and Spark versions, pass the option `-e py39-spark322` or `-e py311-spark344`. All available tox environments containing specific Python and Spark versions can be found in the [tox.ini](/e2e-test/tox.ini) file. To run a specific test, pass the following option: `-- tests/test_data_types.py::test_read_uint64_type`.
