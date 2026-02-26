package tech.ytsaurus.spyt.format

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.YtWriter
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}

class ExtendedYtDistributedWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers with TestUtils {
  behavior of "DistributedWriteOutputCommitProtocol"

  override def reinstantiateSparkSession: Boolean = true

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.yt.write.distributed.enabled", "true")
  }

  it should "append data to an existing table" in withSparkSession() { _spark =>
    _spark.range(100).write.yt(tmpPath)
    _spark.range(100, 200).write.mode(SaveMode.Append).yt(tmpPath)

    val writtenData = readTableAsYson(tmpPath).map(_.asMap().get("id").longValue())
    writtenData should contain theSameElementsAs (0L to 199L)
  }
}
