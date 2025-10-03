package tech.ytsaurus.spyt.format.conf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import tech.ytsaurus.spyt.wrapper.config._

import scala.concurrent.duration.Duration

case class SparkYtWriteConfiguration(bufferSize: Int,
                                     dynBatchSize: Int,
                                     timeout: Duration,
                                     typeV3Format: Boolean,
                                     distributedWrite: Boolean)

object SparkYtWriteConfiguration {

  import SparkYtConfiguration._

  def apply(sqlc: SQLContext): SparkYtWriteConfiguration = SparkYtWriteConfiguration(
    sqlc.ytConf(Write.BufferSize),
    sqlc.ytConf(Write.DynBatchSize),
    sqlc.ytConf(Write.Timeout),
    sqlc.ytConf(Write.TypeV3),
    sqlc.ytConf(Write.Distributed.Enabled)
  )

  def apply(sqlc: SQLConf): SparkYtWriteConfiguration = SparkYtWriteConfiguration(
    sqlc.ytConf(Write.BufferSize),
    sqlc.ytConf(Write.DynBatchSize),
    sqlc.ytConf(Write.Timeout),
    sqlc.ytConf(Write.TypeV3),
    sqlc.ytConf(Write.Distributed.Enabled)
  )
}
