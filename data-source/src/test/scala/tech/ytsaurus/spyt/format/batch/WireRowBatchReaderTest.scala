package tech.ytsaurus.spyt.format.batch

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate
import tech.ytsaurus.spyt.serializers.ArrayAnyDeserializer
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.YtWrapper.formatPath
import tech.ytsaurus.spyt.wrapper.table.{TableIterator, YtReadContext, YtReadSettings}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class WireRowBatchReaderTest extends AnyFlatSpec with Matchers with ReadBatchRows with LocalSpark with TmpDir
  with YtDistributedReadingTestUtils {
  behavior of "WireRowBatchReaderTest"

  import spark.implicits._

  testWithDistributedReading("read table in wire row format") { distributedReadingEnabled =>
    val rowCount = 100
    val batchMaxSize = 10

    val r = new Random()
    val data = Seq.fill(rowCount)((r.nextInt, r.nextDouble))
    val df = data.toDF("a", "b")
    df.coalesce(1).write.yt(tmpPath)

    implicit val ytReadContext: YtReadContext = YtReadContext(yt, YtReadSettings.default)

    val rowIterator: TableIterator[Array[Any]] = if (distributedReadingEnabled) {
      val delegates: Seq[YtPartitionedFileDelegate] = getDelegatesForTable(spark, tmpPath)

      assert(delegates.size == 1)

      YtWrapper.createTablePartitionReader(delegates.head.cookie.get, ArrayAnyDeserializer.getOrCreate(df.schema))
    } else {
      YtWrapper.readTable(YPath.simple(formatPath(tmpPath)),
        ArrayAnyDeserializer.getOrCreate(df.schema), 10 seconds, None, _ => ())
    }

    val reader = new WireRowBatchReader(rowIterator, batchMaxSize, df.schema)
    val res = readFully(reader, df.schema, batchMaxSize)
    val expected = df.collect()

    res should contain theSameElementsAs expected
  }
}
