package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration => SparkSettings}
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.{SparkAdapter, YtDistributedReadingTestUtils, YtReader, YtWriter}

class YtDistributedReadingTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with YtDistributedReadingTestUtils with DynTableTestUtils {

  private val sqlImplicits = SparkAdapter.instance.sparkImplicits(spark)
  import sqlImplicits._

  "YtPartitionedFileDelegate" should "have a cookie when distributed reading is enabled" in {
    val data = (0 until 200).map(x => (x / 200, x / 200, -x))
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(distributedReadingEnabledConf(true)) {
      val delegates: Seq[YtPartitionedFileDelegate] = getDelegatesForTable(spark, tmpPath)
      delegates should not be empty
      all(delegates.map(_.cookie.nonEmpty)) shouldBe true
    }
  }

  it should "fall back to regular reading for ordered dynamic tables" in {
    prepareOrderedTestTable(tmpPath, enableDynamicStoreRead = true)
    val data = (1 to 5).map(i => getTestData(i, i + 2))
    appendChunksToTestTable(tmpPath, data, sorted = false)
    val expected = data.flatten.map(Row.fromTuple)

    Seq(false, true).foreach { ytPartitioningEnabled =>
      withConfs(distributedReadingEnabledConf(true) ++
        Map(s"spark.yt.${SparkSettings.Read.YtPartitioningEnabled.name}" -> ytPartitioningEnabled.toString)) {
        val df = spark.read.option("enable_inconsistent_read", "true").yt(tmpPath)
        df.collect() should contain theSameElementsAs expected
        df.count() shouldBe expected.size

        val delegates = getDelegates(df)
        delegates should not be empty
        all(delegates.map(_.cookie.isEmpty)) shouldBe true
      }
    }
  }

  it should "keep distributed reading for sorted dynamic tables" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))

    withConfs(distributedReadingEnabledConf(true)) {
      val df = spark.read.yt(tmpPath)
      df.collect() should contain theSameElementsAs testData.map(Row.fromTuple)

      val delegates = getDelegates(df)
      delegates should not be empty
      all(delegates.map(_.cookie.nonEmpty)) shouldBe true
    }
  }

  it should "use distributed reading only for static tables when reading them together with ordered dynamic ones" in {
    val staticPath = s"$tmpPath/static"
    val dynamicPath = s"$tmpPath/dynamic"
    val staticData = getTestData(11, 20)
    staticData.toDF().write.yt(staticPath)
    prepareOrderedTestTable(dynamicPath, enableDynamicStoreRead = true)
    appendChunksToTestTable(dynamicPath, Seq(testData), sorted = false)

    withConfs(distributedReadingEnabledConf(true)) {
      val df = spark.read.option("enable_inconsistent_read", "true").yt(staticPath, dynamicPath)
      df.collect() should contain theSameElementsAs (staticData ++ testData).map(Row.fromTuple)

      val (dynamicDelegates, staticDelegates) = getDelegates(df).partition(_.isDynamic)
      staticDelegates should not be empty
      dynamicDelegates should not be empty
      all(staticDelegates.map(_.cookie.nonEmpty)) shouldBe true
      all(dynamicDelegates.map(_.cookie.isEmpty)) shouldBe true
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

}
