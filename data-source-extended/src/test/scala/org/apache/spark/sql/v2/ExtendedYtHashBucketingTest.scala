package org.apache.spark.sql.v2

import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.spyt.types.UInt64Type
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.format.bucketing.HashBucketingTestUtils
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.types.UInt64Long

class ExtendedYtHashBucketingTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir
  with HashBucketingTestUtils {
  behavior of "YtScan"

  private val buckets = 10
  private val bucketedTable = s"$tmpPath-bucketed"

  it should "read each bucket of a UInt64Type hash column into its own partition keyed by the Long bucket value" in {
    awaitTables(startBucketedKeyTable(bucketedTable, buckets, 1L to 24L, valueFactor = 10L))
    withConfs(bucketingConf) {
      val df = catalogTable(bucketedTable)
      df.schema(hashColumn).dataType shouldBe UInt64Type
      val scan = ytScan(df)
      keyGroupedPartitioning(scan).keys().toSeq shouldEqual Seq(Expressions.bucket(buckets, keyColumn))
      shouldKeyPartitionsByBucket(scan, buckets)
      shouldReadRowsIntoTheirBuckets(df, storedRows(bucketedTable)) { row =>
        (row.getAs[UInt64Long](hashColumn).toLong, row.getAs[Long](keyColumn), row.getAs[Long](valueColumn))
      }
    }
  }
}
