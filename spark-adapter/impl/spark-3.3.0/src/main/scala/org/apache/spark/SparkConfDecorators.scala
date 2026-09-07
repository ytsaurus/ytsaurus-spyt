package org.apache.spark

import tech.ytsaurus.spyt.logging.Logging
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.SparkConf")
class SparkConfDecorators {

  @DecoratedMethod
  private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
    val self = __loadFromSystemProperties(silent)
    SparkConfExtensions.loadFromEnvironment(self, silent)
    self
  }

  private[spark] def __loadFromSystemProperties(silent: Boolean): SparkConf = ???
}

@Decorate
@OriginClass("org.apache.spark.SparkConf$")
object SparkConfCompanionDecorators {

  @DecoratedMethod
  def logDeprecationWarning(key: String): Unit = {
    __logDeprecationWarning(key)
    SparkConfExtensions.logDeprecationWarning(key)
  }

  def __logDeprecationWarning(key: String): Unit = ???
}

private[spark] case class DeprecatedConfig(key: String, version: String, deprecationMessage: String)

private[spark] object SparkConfExtensions extends Logging {

  /**
   * Maps deprecated SPYT config keys to information about the deprecation. The warning is logged when the key is
   * set, the same way as SparkConf does it for the deprecated configs of Spark itself.
   */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = Seq(
    DeprecatedConfig("spark.ytsaurus.shuffle.write.config", "2.12.0",
      "Please use spark.ytsaurus.shuffle.config with {pull={writer=...}} instead."),
    DeprecatedConfig("spark.ytsaurus.shuffle.read.config", "2.12.0",
      "Please use spark.ytsaurus.shuffle.config with {pull={reader=...}} instead."),
    DeprecatedConfig("spark.ytsaurus.shuffle.push.config", "2.12.0",
      "Please use spark.ytsaurus.shuffle.config with {push=...} instead.")
  ).map(config => config.key -> config).toMap

  private[spark] def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { config =>
      logWarning(s"The configuration key '$key' has been deprecated as of SPYT ${config.version} " +
        s"and has no effect. ${config.deprecationMessage}")
    }
  }

  private[spark] def loadFromEnvironment(conf: SparkConf, silent: Boolean): SparkConf = {
    val existingEnvNames = conf.getAll.iterator.map { case (k, _) => confToEnvName(k) }.toSet
    for ((key, value) <- sys.env if isSparkEnv(key)) {
      if (!existingEnvNames.contains(sparkEnvName(key))) {
        conf.set(envToConfName(key), value, silent)
      }
    }
    conf
  }

  private[spark] def envToConfName(envName: String): String = {
    sparkEnvName(envName).toLowerCase().replace("_", ".")
  }

  private def sparkEnvName(envName: String): String = {
    if (envName.startsWith(SECURE_VAULT_ENV_PREFIX)) {
      envName.substring(SECURE_VAULT_CUT_LENGTH)
    } else {
      envName
    }
  }

  private def isSparkEnv(key: String): Boolean = {
    key.startsWith(SPARK_ENV_PREFIX) || key.startsWith(SECURE_VAULT_ENV_PREFIX)
  }

  private[spark] def confToEnvName(confName: String): String = {
    confName.replace(".", "_").toUpperCase()
  }

  private val SPARK_ENV_PREFIX = "SPARK_"
  private val SECURE_VAULT_ENV_PREFIX = "YT_SECURE_VAULT_SPARK_"
  private val SECURE_VAULT_CUT_LENGTH = SECURE_VAULT_ENV_PREFIX.length - SPARK_ENV_PREFIX.length
}
