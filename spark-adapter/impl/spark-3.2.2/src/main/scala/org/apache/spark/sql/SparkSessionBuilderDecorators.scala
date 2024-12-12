package org.apache.spark.sql

import tech.ytsaurus.spyt.adapter.StorageSupport
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.SparkSession$Builder")
class SparkSessionBuilderDecorators {

  @DecoratedMethod
  def getOrCreate(): SparkSession = {
    val spark = __getOrCreate()
    spark.experimental.extraOptimizations ++= StorageSupport.instance.createExtraOptimizations(spark)
    spark
  }

  def __getOrCreate(): SparkSession = ???
}
