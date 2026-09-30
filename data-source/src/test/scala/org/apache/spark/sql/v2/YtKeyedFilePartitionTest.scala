package org.apache.spark.sql.v2

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class YtKeyedFilePartitionTest extends AnyFlatSpec with Matchers {
  behavior of "YtKeyedFilePartition"

  it should "keep its partition key in a copy" in {
    val files = Array.empty[PartitionedFile]
    val key = new GenericInternalRow(Array[Any](1L))
    val copied = new YtKeyedFilePartition(0, files, key).copy(index = 3)

    copied shouldBe a[YtKeyedFilePartition]
    copied.index shouldBe 3
    copied.files should be theSameInstanceAs files
    copied.asInstanceOf[YtKeyedFilePartition].partitionKey() shouldBe key
  }
}
