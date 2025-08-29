
package org.apache.spark.deploy.ytsaurus

import org.apache.spark.internal.config.ConfigBuilder
import tech.ytsaurus.spyt.BuildInfo
import tech.ytsaurus.spyt.wrapper.config.Utils.releaseTypeDirectory

import java.util.concurrent.TimeUnit

object Config {
  val GLOBAL_CONFIG_PATH = ConfigBuilder("spark.ytsaurus.config.global.path")
    .doc("Path to global Spark configuration for the whole YTsaurus cluster")
    .version("1.76.0")
    .stringConf
    .createWithDefault("//home/spark/conf/global")

  val RELEASE_CONFIG_PATH = ConfigBuilder("spark.ytsaurus.config.releases.path")
    .doc("Root path for SPYT releases configuration")
    .version("1.76.0")
    .stringConf
    .createWithDefault(s"//home/spark/conf/${releaseTypeDirectory(BuildInfo.version)}")

  val SPARK_DISTRIBUTIVES_PATH = ConfigBuilder("spark.ytsaurus.distributives.path")
    .doc("Root path for Spark distributives")
    .version("2.0.0")
    .stringConf
    .createWithDefault("//home/spark/distrib")

  val LAUNCH_CONF_FILE = ConfigBuilder("spark.ytsaurus.config.launch.file")
    .doc("SPYT release configuration file name")
    .version("1.76.0")
    .stringConf
    .createWithDefault("spark-launch-conf")

  val SPYT_VERSION = ConfigBuilder("spark.ytsaurus.spyt.version")
    .doc("SPYT version to use on cluster")
    .version("1.76.0")
    .stringConf
    .createOptional

  val YTSAURUS_MAX_DRIVER_FAILURES = ConfigBuilder("spark.ytsaurus.driver.maxFailures")
    .doc("Maximum driver task failures before operation failure")
    .version("1.76.0")
    .intConf
    .createWithDefault(5)

  // From Spark 3.5.0 there also exists an option spark.executor.maxNumFailures which also resolves to constant
  // MAX_EXECUTOR_FAILURES
  val YTSAURUS_MAX_EXECUTOR_FAILURES = ConfigBuilder("spark.ytsaurus.executor.maxFailures")
    .doc("Maximum executor task failures before operation failure")
    .version("1.76.0")
    .intConf
    .createWithDefault(10)

  val EXECUTOR_OPERATION_SHUTDOWN_DELAY = ConfigBuilder("spark.ytsaurus.executor.operation.shutdown.delay")
    .doc("Time for executors to shutdown themselves before terminating the executor operation, milliseconds")
    .version("1.76.0")
    .longConf
    .createWithDefault(10000)

  val YTSAURUS_POOL = ConfigBuilder("spark.ytsaurus.pool")
    .doc("YTsaurus scheduler pool to execute this job")
    .version("1.78.0")
    .stringConf
    .createOptional

  val YTSAURUS_IS_PYTHON = ConfigBuilder("spark.ytsaurus.isPython")
    .internal()
    .version("1.78.0")
    .booleanConf
    .createWithDefault(false)

  val YTSAURUS_IS_PYTHON_BINARY = ConfigBuilder("spark.ytsaurus.isPythonBinary")
    .internal()
    .version("2.4.0")
    .booleanConf
    .createWithDefault(false)

  val YTSAURUS_PYTHON_BINARY_ENTRY_POINT = ConfigBuilder("spark.ytsaurus.python.binary.entry.point")
    .doc("An entry point for python binary for cluster mode if it's not main method. It is taken from " +
      "Y_PYTHON_ENTRY_POINT environment variable if not explicitly specified as spark-submit --conf parameter. " +
      "For client mode Y_PYTHON_ENTRY_POINT environment variable should be used.")
    .version("2.4.0")
    .stringConf
    .createOptional

  val YTSAURUS_PYTHON_EXECUTABLE = ConfigBuilder("spark.ytsaurus.python.executable")
    .internal()
    .version("1.78.0")
    .stringConf
    .createOptional

  val TCP_PROXY_ENABLED = ConfigBuilder("spark.ytsaurus.tcp.proxy.enabled")
    .version("2.1.0")
    .booleanConf
    .createWithDefault(false)

  val TCP_PROXY_RANGE_START = ConfigBuilder("spark.ytsaurus.tcp.proxy.range.start")
    .version("2.1.0")
    .intConf
    .createWithDefault(30000)

  val TCP_PROXY_RANGE_SIZE = ConfigBuilder("spark.ytsaurus.tcp.proxy.range.size")
    .version("2.1.0")
    .intConf
    .createWithDefault(1000)

  val YTSAURUS_CUDA_VERSION = ConfigBuilder("spark.ytsaurus.cuda.version")
    .version("2.1.0")
    .stringConf
    .createOptional

  val DRIVER_OPERATION_ID = "spark.ytsaurus.driver.operation.id"
  val SUBMISSION_ID = "spark.ytsaurus.submission.id"
  val EXECUTOR_OPERATION_ID = "spark.ytsaurus.executor.operation.id"
  val SPARK_PRIMARY_RESOURCE = "spark.ytsaurus.primary.resource"

  val YTSAURUS_REDIRECT_STDOUT_TO_STDERR = ConfigBuilder("spark.ytsaurus.redirect.stdout.to.stderr")
    .version("2.1.0")
    .booleanConf
    .createWithDefault(false)

  val YTSAURUS_PORTO_LAYER_PATHS = ConfigBuilder("spark.ytsaurus.porto.layer.paths")
    .version("2.1.0")
    .stringConf
    .createOptional

  val YTSAURUS_EXTRA_PORTO_LAYER_PATHS = ConfigBuilder("spark.ytsaurus.porto.extra.layer.paths")
    .version("2.1.0")
    .stringConf
    .createOptional

  val YTSAURUS_DRIVER_OPERATION_DUMP_PATH = ConfigBuilder("spark.ytsaurus.driver.operation.dump.path")
    .doc("File where driver operation id will be written after successful start")
    .version("2.2.0")
    .stringConf
    .createOptional

  val YTSAURUS_REMOTE_TEMP_FILES_DIRECTORY = ConfigBuilder("spark.ytsaurus.remote.temp.files.directory")
    .doc("Path to temporary directory on Cypress for uploading local files and file cache")
    .version("2.4.0")
    .stringConf
    .createWithDefault("//tmp/yt_wrapper/file_storage")

  val SPYT_ANNOTATIONS = "spark.ytsaurus.annotations"
  val SPYT_DRIVER_ANNOTATIONS = "spark.ytsaurus.driver.annotations"
  val SPYT_EXECUTORS_ANNOTATIONS = "spark.ytsaurus.executors.annotations"

  val YTSAURUS_DRIVER_WATCH = ConfigBuilder("spark.ytsaurus.driver.watch")
    .doc("Enable logging for driver operation")
    .version("2.4.2")
    .booleanConf
    .createWithDefault(true)

  val YTSAURUS_NETWORK_PROJECT = ConfigBuilder("spark.ytsaurus.network.project")
    .doc("Network project name")
    .version("2.4.3")
    .stringConf
    .createOptional

  val YTSAURUS_SQUASHFS_ENABLED = ConfigBuilder("spark.ytsaurus.squashfs.enabled")
    .version("2.6.0")
    .booleanConf
    .createWithDefault(false)

  val YTSAURUS_METRICS_ENABLED = ConfigBuilder("spark.ytsaurus.metrics.enabled")
    .version("2.7.0")
    .booleanConf
    .createWithDefault(false)

  val YT_METRICS_PULL_PORT = ConfigBuilder("spark.ytsaurus.metrics.pull.port")
    .version("2.7.0")
    .intConf
    .createWithDefault(27100)

  val YT_METRICS_AGENT_PULL_PORT = ConfigBuilder("spark.ytsaurus.metrics.agent.pull.port")
    .version("2.7.0")
    .intConf
    .createWithDefault(27101)

  val YTSAURUS_CLIENT_TIMEOUT = ConfigBuilder("spark.ytsaurus.client.rpc.timeout")
    .version("2.6.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createOptional

  val YTSAURUS_RPC_JOB_PROXY_ENABLED = ConfigBuilder("spark.ytsaurus.rpc.job.proxy.enabled")
    .version("2.6.0")
    .booleanConf
    .createWithDefault(true)

  val YTSAURUS_JAVA_HOME = ConfigBuilder("spark.ytsaurus.java.home")
    .doc("Path to java home on YTsaurus cluster")
    .version("2.6.0")
    .stringConf
    .createWithDefault(s"/opt/jdk${Runtime.version().feature()}")

  val YTSAURUS_SHUFFLE_ENABLED = ConfigBuilder("spark.ytsaurus.shuffle.enabled")
    .version("2.7.2")
    .booleanConf
    .createWithDefault(false)
}
