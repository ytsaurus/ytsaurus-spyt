package org.apache.spark.sql.execution.datasources.v2

import tech.ytsaurus.spyt.patch.annotations.{AddInterfaces, OriginClass}

import java.io.Serializable


@AddInterfaces(Array(classOf[Serializable]))
@OriginClass("org.apache.spark.sql.execution.datasources.v2.DataSourcePartitioning")
class DataSourcePartitioningDecorator {

}
