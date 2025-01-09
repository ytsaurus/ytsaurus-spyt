package org.apache.spark.sql.yt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.sql.yt.ReadTransactionStrategy.ypathEnriched
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.{YtReader, YtWriter}

import java.util.UUID
import scala.util.Using

class ReadTransactionStrategyTest extends FlatSpec with Matchers with LocalSpark with TestUtils with TmpDir with DynTableTestUtils {
  it should "be able to read removed table" in {
    val tmpPath = s"$testDir/test-${UUID.randomUUID()}"

    {
      import spark.implicits._

      Seq(Seq(1, 2, 3)).toDF().coalesce(1).write.mode("overwrite").yt(tmpPath)
    }

    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()

    Using.resource(
      sparkSessionBuilder.withExtensions { extensions =>
        new tech.ytsaurus.spyt.format.YtSparkExtensions().apply(extensions)
        extensions.injectPreCBORule { _ =>
          plan => {
            val tables = plan.collect {
              case DataSourceV2Relation(table: YtTable, _, _, _, _) => table
            }
            if (tables.nonEmpty) {
              assertResult(null)(tables.find { table =>
                table.paths.exists(ypathEnriched(_).transaction.isEmpty)
              }.orNull)
              yt.removeNode(tmpPath).get()
            }
            plan
          }
        }
      }.getOrCreate()
    ) { spark =>
      assertResult(1)(spark.read.yt(tmpPath).count())
    }
  }
}
