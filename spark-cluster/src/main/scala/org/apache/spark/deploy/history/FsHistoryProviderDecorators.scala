package org.apache.spark.deploy.history

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.YtHistoryServer.Config.CREATE_LOG_DIR
import tech.ytsaurus.spyt.SparkVersionUtils
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

import java.util

@Decorate
@OriginClass("org.apache.spark.deploy.history.FsHistoryProvider")
class FsHistoryProviderDecorators {

  @DecoratedMethod
  private def startPolling(): Unit = {
    val path = new Path(logDir)
    val confFieldName = if (SparkVersionUtils.lessThan("3.3.0")) {
      "conf"
    } else {
      "org$apache$spark$deploy$history$FsHistoryProvider$$conf"
    }

    val conf: SparkConf = this.getClass.getDeclaredField(confFieldName).get(this).asInstanceOf[SparkConf]

    if (!fs.exists(path) && conf.get(CREATE_LOG_DIR)) {
      fs.mkdirs(path)
    }

    __startPolling()
  }

  private def __startPolling(): Unit = ???
  private val logDir: String = ???
  private[history] val fs: FileSystem = ???
}
