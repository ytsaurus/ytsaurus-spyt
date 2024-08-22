package tech.ytsaurus.spyt.format

import org.apache.spark.test.UtilsWrapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}

class WriteStatisticsTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  import spark.implicits._

  it should "count output statistics" in {
    val rowCount = 9000
    val data = Stream.from(1).take(rowCount).map(a => (a, a + 1, 5)).toDF().repartition(100)
    val rowSize = 3 * 8 // 3 x 64-bit numbers
    val dataSize = rowCount * rowSize

    val store = UtilsWrapper.appStatusStore(spark)
    val totalOutputBefore = store.stageList(null).map(_.outputBytes).sum

    data.write.yt(tmpPath)

    val totalOutput = store.stageList(null).map(_.outputBytes).sum - totalOutputBefore
    totalOutput should be >= 1L * dataSize
    totalOutput should be < 3L * dataSize  // Writing will have overhead for encoding schema, query params
  }
}
