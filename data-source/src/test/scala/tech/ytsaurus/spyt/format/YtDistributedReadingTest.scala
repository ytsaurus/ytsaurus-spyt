package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration => SparkSettings}
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.{YtDistributedReadingTestUtils, YtReader, YtWriter}

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

  it should "pushdown filters" in {
    val data = (1L to 1000L).map(x => (x, x % 2))
    val df = data.toDF("a", "b").repartition(2)
    df.sort("a","b").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(distributedReadingEnabledConf(true) ++
      Map(s"spark.yt.${SparkSettings.Read.KeyColumnsFilterPushdown.Enabled.name}" -> "true")) {
      val resDf = spark.read.yt(tmpPath).select("a","b").filter("a >= 49 AND a <= 50 AND b == 1")
      val res = resDf.collect()
      val expectedData = data.filter { case (a, b) => a >= 49 && a <= 50 && b == 1 }

      scanOutputRows(resDf) should equal(2) // number of rows read from YT with pushdown filters
      res should contain theSameElementsAs expectedData.map(Row.fromTuple)
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
