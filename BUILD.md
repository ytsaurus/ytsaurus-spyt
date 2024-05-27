## Building YTsaurus SPYT from sources

### Build Requirements

- jdk11 (openjdk is preferred)
- sbt 1.5.4
- python 3.8+
- pip3
- wheel

### How to Build

To build a snapshot version of YTsaurus the following sbt command can be used:

```bash
sbt -DpublishYt=false clean spytPublishSnapshot
```

This will create a SPYT assembly archive and Python wheel package inside `build_output` directory. After that you can install the whl package into your Python environment and deploy the assembly to the cluster.

Installing Python whl package:

```bash
pip install `ls build_output/ytsaurus-spyt/*.whl`
```

### Running tests

#### Prerequisites

Almost all unit tests are using local YTsaurus cluster in Docker as described here: https://ytsaurus.tech/docs/overview/try-yt#using-docker. The local cluster must be started with at least one RPC proxy. This can be done with parameter `--rpc-proxy-count 1`

An example command for starting the local cluster with one RPC proxy:

```bash
run_local_cluster.sh --rpc-proxy-count 1
```

After finishing working with unit tests you can stop the local YTsaurus cluster with the following command:

```bash
run_local_cluster.sh --stop
```

#### Running scala unit tests

To launch scala unit tests the following command should be used from the root of the project (assuming that the local YTsaurus cluster is launched):

```bash
sbt test
```

To run a specific test class a `testOnly` sbt command can be used:

```bash
sbt "<project module>/testOnly <full test class name>"
```

An example:

```bash
sbt "data-source/testOnly tech.ytsaurus.spyt.format.types.DateTimeTypesConverterTest"
```

#### Running python unit tests

Prerequisites for running python unit tests:
- python3.9, python3.11 or python3.12
- tox >= 4
- wheel
- running local YTsaurus cluster (see above)

Python unit tests should be run under `spyt-package/src/test/python` directory. Tests are run using [tox](https://tox.wiki) python environment management tool, but you can use `run-tests.sh` script for convenience.

`run-tests.sh` rebuilds the project by default, but if you have already built the project you can skip build stage by passing `--no-rebuild` flag to the script.

To run on specific python version pass the option `-e py39` or `-e py311`. To run a specific test pass the following option: `-- pytest/test_data_types.py::test_read_uint64_type`.
