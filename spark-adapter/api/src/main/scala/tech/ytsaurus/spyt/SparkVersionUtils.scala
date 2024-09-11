package tech.ytsaurus.spyt

object SparkVersionUtils {
  val currentVersion: String = org.apache.spark.SPARK_VERSION

  private def parseSparkVersion(sparkVersion: String): (Int, Int, Int) = {
    val sparkVersionComponents = sparkVersion.split('.')
    if (sparkVersionComponents.length != 3) {
      throw new IllegalArgumentException("ytsaurus-spyt supports only release Spark versions")
    }

    (sparkVersionComponents(0).toInt, sparkVersionComponents(1).toInt, sparkVersionComponents(2).toInt)
  }

  val ordering: Ordering[String] = (v1: String, v2: String) => {
    Ordering.Tuple3(Ordering.Int, Ordering.Int, Ordering.Int).compare(parseSparkVersion(v1), parseSparkVersion(v2))
  }

  def lessThan(sparkVersion: String): Boolean = ordering.lt(currentVersion, sparkVersion)
}
