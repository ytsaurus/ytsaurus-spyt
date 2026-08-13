package org.apache.spark.sql.v2

import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockUpdated}
import org.apache.spark.sql.v2.Utils.extractYtScan
import org.apache.spark.storage.BroadcastBlockId
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.KeyColumnsFilterPushdown
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.config._
import tech.ytsaurus.spyt.{YtReader, YtWriter}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class YtScanBroadcastTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir {
  behavior of "YtScan"

  override def reinstantiateSparkSession: Boolean = true

  private val listenerTimeoutMillis = 60000
  private val readerFactoryCount = 10

  it should "not broadcast hadoop configuration again for every reader factory" in withSparkSession() { _spark =>
    val seenBroadcasts = ConcurrentHashMap.newKeySet[Long]()
    _spark.sparkContext.addSparkListener(new SparkListener {
      override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
        event.blockUpdatedInfo.blockId match {
          case id: BroadcastBlockId if event.blockUpdatedInfo.storageLevel.isValid =>
            seenBroadcasts.add(id.broadcastId)
          case _ =>
        }
      }
    })

    import _spark.implicits._
    Seq(1, 2, 3).toDF("a").write.yt(tmpPath)

    val df = _spark.read.yt(tmpPath)
    df.collect()
    val scan = extractYtScan(df.queryExecution.executedPlan)

    _spark.sparkContext.listenerBus.waitUntilEmpty(listenerTimeoutMillis)
    val broadcastsBefore = seenBroadcasts.asScala.toSet

    (1 to readerFactoryCount).foreach(_ => scan.createReaderFactory())
    _spark.sparkContext.listenerBus.waitUntilEmpty(listenerTimeoutMillis)

    (seenBroadcasts.asScala.toSet -- broadcastsBefore) shouldBe empty
  }

  it should "see configuration changed at runtime in scans created afterwards" in withSparkSession() { _spark =>
    import _spark.implicits._
    Seq(1, 2, 3).toDF("a").write.yt(tmpPath)

    val dfBefore = _spark.read.yt(tmpPath)
    dfBefore.collect()
    val scanBefore = extractYtScan(dfBefore.queryExecution.executedPlan)
    scanBefore.createReaderFactory()
    scanBefore.description() should include("filter pushdown enabled: false")

    _spark.setYtConf(KeyColumnsFilterPushdown.Enabled, true)

    val dfAfter = _spark.read.yt(tmpPath)
    dfAfter.collect()
    val scanAfter = extractYtScan(dfAfter.queryExecution.executedPlan)
    scanAfter.description() should include("filter pushdown enabled: true")
  }
}
