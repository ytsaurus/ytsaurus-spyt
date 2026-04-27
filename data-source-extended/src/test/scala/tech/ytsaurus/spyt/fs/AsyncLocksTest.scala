package tech.ytsaurus.spyt.fs

import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.LockNode
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

class AsyncLocksTest extends AnyFlatSpec with Matchers with LocalSpark with TestUtils with TmpDir {

  it should "acquire locks asynchronously when reading directory with many tables" in {
    withSparkSession(Map("spark.yt.read.transactional" -> "true")) { _spark =>
      YtWrapper.createDir(tmpPath)
      val schema = TableSchema.builder().addValue("a", ColumnValueType.INT64).build()
      val tableCount = 10
      (1 to tableCount).foreach { i =>
        writeTableFromYson(Seq(s"{a = ${i.toLong}}"), s"$tmpPath/table$i", schema)
      }

      val maxInFlight = withLockNodeSpyYt(_spark) {
        _spark.read.yt(tmpPath).collect()
      }

      maxInFlight should be > 1
    }
  }

  private def withLockNodeSpyYt(_spark: org.apache.spark.sql.SparkSession)(body: => Unit): Int = {
    val rpcClient = YtClientProvider.ytRpcClient(ytClientConfiguration(_spark))
    val spyYt: CompoundClient = Mockito.spy(rpcClient.yt)
    val inFlight = new AtomicInteger(0)
    val maxInFlight = new AtomicInteger(0)

    Mockito.doAnswer { invocation =>
      val realFuture = invocation.callRealMethod().asInstanceOf[CompletableFuture[_]]
      val current = inFlight.incrementAndGet()
      maxInFlight.accumulateAndGet(current, Math.max)
      realFuture.whenComplete { (_, _) =>
        inFlight.decrementAndGet()
      }
      realFuture
    }.when(spyYt).lockNode(ArgumentMatchers.any(classOf[LockNode]))

    try {
      YtClientProvider.getClients(s"${rpcClient.normalizedProxy};") = rpcClient.copy(yt = spyYt)
      body
    } finally {
      YtClientProvider.getClients(s"${rpcClient.normalizedProxy};") = rpcClient
    }

    maxInFlight.get()
  }
}
