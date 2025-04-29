package org.apache.spark

import org.apache.spark.deploy.YTsaurusConstants.YTSAURUS_MASTER
import org.apache.spark.internal.config.{DRIVER_CORES, SUBMIT_DEPLOY_MODE}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.SparkContext$")
object SparkContextDecorators {

  @DecoratedMethod
  private[spark] def numDriverCores(master: String, conf: SparkConf): Int = master match {
    case YTSAURUS_MASTER =>
      if (conf != null && conf.get(SUBMIT_DEPLOY_MODE) == "cluster") {
        conf.getInt(DRIVER_CORES.key, 0)
      } else {
        1
      }
    case _ => __numDriverCores(master, conf)
  }

  private[spark] def __numDriverCores(master: String, conf: SparkConf): Int = ???

}