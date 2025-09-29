package tech.ytsaurus.spyt.format

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.{YtDistributedReadingTestUtils, YtWriter}

class YtDistributedReadingTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with YtDistributedReadingTestUtils with DynTableTestUtils {

  import spark.implicits._

  "YtPartitionedFileDelegate" should "have distributedReadingEnabled = true and not empty serializedCookie" in {
    val data = (0 until 200).map(x => (x / 200, x / 200, -x))
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(distributedReadingEnabledConf(true)) {
      val delegates: Seq[YtPartitionedFileDelegate] = getDelegatesForTable(spark, tmpPath)
      delegates.forall(d => d.distributedReadingEnabled && d.cookie.nonEmpty)
    }
  }

  it should "fail for ordered dynamic table" in {
    import tech.ytsaurus.spyt._
    prepareOrderedTestTable(tmpPath, enableDynamicStoreRead = true)
    a[Exception] shouldBe thrownBy {
      withConfs(distributedReadingEnabledConf(true)) {
        spark.read.option("enable_inconsistent_read", "true").yt(tmpPath).collect()
      }
    }
  }

}
