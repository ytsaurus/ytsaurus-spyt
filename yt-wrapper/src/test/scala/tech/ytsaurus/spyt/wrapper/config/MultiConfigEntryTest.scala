package tech.ytsaurus.spyt.wrapper.config

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.{FlatSpec, Matchers}

class MultiConfigEntryTest extends FlatSpec with Matchers {
  import ConfigEntry.implicits._

  private val entry = new MultiConfigEntry("log", "level", Some(Level.INFO),
    { (name: String, default: Option[Level]) => new ConfigEntry[Level](name, default) })

  private def test(confProvider: ConfProvider): Unit = {
    val res = confProvider.ytConf(entry)

    res("test1") shouldEqual Level.INFO
    res("test2") shouldEqual Level.WARN
  }

  "MultiConfigEntry" should "get value from SparkConf" in {
    val sparkConf = new SparkConf
    sparkConf.set("spark.yt.log.level", "INFO")
    sparkConf.set("spark.yt.log.test2.level", "WARN")

    test(sparkConf)
  }

  it should "get value from hadoop configuration" in {
    val hadoopConf = new Configuration()
    hadoopConf.set("yt.log.level", "INFO")
    hadoopConf.set("yt.log.test2.level", "WARN")

    test(hadoopConf)
  }

  it should "get value from spark and sql context" in {
    val sparkConf = new SparkConf
    sparkConf.set("spark.yt.log.level", "INFO")
    sparkConf.set("spark.yt.log.test2.level", "WARN")
    val spark = SparkSession.builder().master("local").config(sparkConf).getOrCreate()
    try {
      test(spark.sqlContext)
      test(spark)
    } finally {
      spark.stop()
    }
  }

  it should "get value from map" in {
    val options = Map(
      "log.level" -> "INFO",
      "log.test2.level" -> "WARN"
    )

    test(options)
  }

  it should "get value from case insensitive map" in {
    import scala.collection.JavaConverters._
    val options = new CaseInsensitiveStringMap(Map(
      "log.level" -> "INFO",
      "log.test2.level" -> "WARN"
    ).asJava)

    test(options)
  }

}
