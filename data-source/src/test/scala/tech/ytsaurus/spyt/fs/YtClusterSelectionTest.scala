package tech.ytsaurus.spyt.fs

import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, LocalYt, TmpDir}

class YtClusterSelectionTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir {
  import spark.implicits._

  it should "read with cluster specification" in {
    val df = Seq((1, "a"), (2, "d")).toDF("num", "str")
    df.write.yt(tmpPath)

    val res = spark.read.yt(s"""<cluster="${LocalYt.proxy}">$tmpPath""")
    res.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "d"))

    val res2 = spark.read.format("yt").load(s"""ytTable:/<cluster="${LocalYt.proxy}">$tmpPath""")
    res2.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "d"))
  }

  it should "join tables" in {
    Seq((1, "a"), (2, "d")).toDF("num", "str").write.yt(tmpPath)
    Seq((1, 0.0), (2, 0.1)).toDF("num", "double").write.yt(s"$tmpPath-2")

    val res = spark.read.yt(tmpPath)
      .join(spark.read.yt(s"""<cluster="${LocalYt.proxy}">$tmpPath-2"""), "num")

    res.collect() should contain theSameElementsAs Seq(Row(1, "a", 0.0), Row(2, "d", 0.1))
  }
}
