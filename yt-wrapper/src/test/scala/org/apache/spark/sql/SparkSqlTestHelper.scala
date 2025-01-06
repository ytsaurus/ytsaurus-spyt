package org.apache.spark.sql

/**
 * The main class purpose is to increase visibility of some Spark package-private methods
 */
object SparkSqlTestHelper {
  def showString(df: Dataset[_], numRows: Int, truncate: Int = 20): String = df.showString(numRows, truncate)
}
