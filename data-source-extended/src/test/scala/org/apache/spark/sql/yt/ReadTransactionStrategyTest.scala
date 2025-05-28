package org.apache.spark.sql.yt

import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.Utils.tryWithResources

class ReadTransactionStrategyTest extends FlatSpec with Matchers with LocalSpark with TestUtils with TmpDir with DynTableTestUtils {
  override def reinstantiateSparkSession: Boolean = true

  it should "be able to read removed table" in {
    writeTableFromYson(Seq("""{}"""), tmpPath, TableSchema.builder().build())
    tryWithResources(sparkSessionBuilder().withExtensions { extensions =>
      extensions.injectPreCBORule(new ReadTransactionStrategy(_))
      extensions.injectPreCBORule { _ =>
        plan => {
          yt.removeNode(tmpPath).get() // ReadTransactionStrategy should snapshot-lock the table at this point
          plan
        }
      }
    }.getOrCreate()) { spark =>
      spark.read.yt(tmpPath).count() shouldBe 1
      yt.existsNode(tmpPath).get() shouldBe false
    }
  }
}
