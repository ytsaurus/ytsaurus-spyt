package org.apache.spark

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

private[spark] object SparkConfExtensions {
  private[spark] def loadFromEnvironment(conf: SparkConf, silent: Boolean): SparkConf = {
    for ((key, value) <- sys.env if isSparkEnv(key)) {
      conf.set(SparkConfExtensions.envToConfName(key), value, silent)
    }
    conf
  }

  private[spark] def envToConfName(envName: String): String = {
    val canonicEnvName = if (envName.startsWith(SECURE_VAULT_ENV_PREFIX)) {
      envName.substring(SECURE_VAULT_CUT_LENGTH)
    } else {
      envName
    }
    canonicEnvName.toLowerCase().replace("_", ".")
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
