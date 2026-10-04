package tech.ytsaurus.spyt.format

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.YtWrite
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.client.ApiServiceTransaction
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.request.LockMode
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.exceptions.InconsistentDynamicWriteException
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.time.Duration

class ExternalTransactionTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {

  override def reinstantiateSparkSession: Boolean = true

  behavior of "External transaction defaults"

  for (sorted <- Seq(false, true); commit <- Seq(false, true)) {
    it should s"overwrite locked tables atomically: sorted=$sorted, commit=$commit" in {
      val copiedTablePath = s"${tmpPath}_second"
      try {
        withExternalTransaction() { (session, externalTransaction) =>
          val transactionId = externalTransaction.getId.toString
          val schema = TableSchema.builder().addValue("id", ColumnValueType.INT64).build()
          writeTableFromYson(Seq("{id=0}"), tmpPath, schema)
          YtWrapper.lockNodeAsync(YPath.simple(tmpPath), transactionId, LockMode.Exclusive).join()
          val writer = session.range(2, 4).write.mode(SaveMode.Overwrite)
          (if (sorted) writer.sortedBy("id") else writer).yt(tmpPath)
          session.read.yt(tmpPath).collect().toSeq should contain theSameElementsAs Seq(Row(2L), Row(3L))
          readIds(tmpPath, Some(transactionId)) should contain theSameElementsAs Seq(2L, 3L)
          readIds(tmpPath) shouldEqual Seq(0L)
          session.read.yt(tmpPath).write.yt(copiedTablePath)
          readIds(copiedTablePath, Some(transactionId)) should contain theSameElementsAs Seq(2L, 3L)
          YtWrapper.exists(copiedTablePath) shouldBe false
          YtWrapper.transactionExists(transactionId) shouldBe true
          if (commit) externalTransaction.commit().join() else externalTransaction.abort().join()
          val expectedIds = if (commit) Seq(2L, 3L) else Seq(0L)
          readIds(tmpPath) should contain theSameElementsAs expectedIds
          YtWrapper.exists(copiedTablePath) shouldBe commit
          if (commit) {
            readIds(copiedTablePath) should contain theSameElementsAs Seq(2L, 3L)
          }
        }
      } finally {
        YtWrapper.remove(copiedTablePath, force = true)
      }
    }
  }

  for (distributed <- Seq(false, true)) {
    it should s"read uncommitted tables with YT partitioning: distributed=$distributed" in {
      withExternalTransaction() { (session, _) =>
        session.conf.set("spark.yt.read.ytPartitioning.enabled", "true")
        session.conf.set("spark.yt.read.ytDistributedReading.enabled", distributed.toString)
        session.range(3).write.yt(tmpPath)
        session.read.yt(tmpPath).collect().toSeq should contain theSameElementsAs Seq(Row(0L), Row(1L), Row(2L))
        YtWrapper.exists(tmpPath) shouldBe false
      }
    }
  }

  it should "overwrite a table created earlier in the parent transaction" in {
    withExternalTransaction() { (session, _) =>
      session.range(1).write.yt(tmpPath)
      session.range(2, 3).write.mode(SaveMode.Overwrite).yt(tmpPath)
      session.read.yt(tmpPath).collect().toSeq shouldEqual Seq(Row(2L))
      YtWrapper.exists(tmpPath) shouldBe false
    }
  }

  for (mode <- Seq(SaveMode.Overwrite, SaveMode.Append)) {
    it should s"preserve the external read snapshot and reject a subsequent write in mode $mode" in {
      val schema = TableSchema.builder().addValue("id", ColumnValueType.INT64).build()
      writeTableFromYson(Seq("{id=0}"), tmpPath, schema)
      withExternalTransaction() { (session, externalTransaction) =>
        val transactionId = externalTransaction.getId.toString
        val input = session.read.yt(tmpPath)
        YtWrapper.attribute(tmpPath, "lock_mode", Some(transactionId)).stringValue() shouldBe "snapshot"
        overwriteTableFromYson(Seq("{id=1}"), tmpPath, schema)
        readIds(tmpPath) shouldEqual Seq(1L)
        input.collect().toSeq shouldEqual Seq(Row(0L))
        val error = intercept[Exception] {
          session.range(2, 3).write.mode(mode).yt(tmpPath)
        }
        error.getMessage should include ("snapshot")
        error.getMessage should include ("parent transaction")
        error.getMessage should include (transactionId)
        input.collect().toSeq shouldEqual Seq(Row(0L))
        readIds(tmpPath) shouldEqual Seq(1L)
        YtWrapper.transactionExists(transactionId) shouldBe true
      }
    }

    it should s"write in mode $mode after reading a table locked by the parent" in {
      val schema = TableSchema.builder().addValue("id", ColumnValueType.INT64).build()
      writeTableFromYson(Seq("{id=0}"), tmpPath, schema)
      withExternalTransaction() { (session, externalTransaction) =>
        val transactionId = externalTransaction.getId.toString
        YtWrapper.lockNodeAsync(YPath.simple(tmpPath), transactionId, LockMode.Exclusive).join()
        session.read.yt(tmpPath).collect().toSeq shouldEqual Seq(Row(0L))
        session.range(2, 3).write.mode(mode).yt(tmpPath)
        val expectedIds = if (mode == SaveMode.Overwrite) Seq(2L) else Seq(0L, 2L)
        readIds(tmpPath, Some(transactionId)) should contain theSameElementsAs expectedIds
        readIds(tmpPath) shouldEqual Seq(0L)
        externalTransaction.commit().join()
        readIds(tmpPath) should contain theSameElementsAs expectedIds
      }
    }
  }

  for (mode <- Seq(SaveMode.Overwrite, SaveMode.ErrorIfExists)) {
    it should s"reject an unapplied session transaction before writing in mode $mode" in {
      withSparkSession(Map("spark.sql.extensions" -> "")) { baseSession =>
        withExternalTransaction(baseSession) { (session, externalTransaction) =>
          val transactionId = externalTransaction.getId.toString
          session.range(1).write.option("write_transaction", transactionId).yt(tmpPath)
          val error = intercept[IllegalStateException] {
            session.range(2, 3).write.mode(mode).yt(tmpPath)
          }
          error.getMessage should include ("was not applied during output planning")
          readIds(tmpPath, Some(transactionId)) shouldEqual Seq(0L)
          YtWrapper.exists(tmpPath) shouldBe false
        }
      }
    }
  }

  it should "reject an unapplied session transaction before Ignore checks output existence" in {
    val schema = TableSchema.builder().addValue("id", ColumnValueType.INT64).build()
    writeTableFromYson(Seq("{id=0}"), tmpPath, schema)
    withSparkSession(Map("spark.sql.extensions" -> "")) { baseSession =>
      withExternalTransaction(baseSession) { (session, externalTransaction) =>
        val transactionId = externalTransaction.getId.toString
        YtWrapper.remove(tmpPath, Some(transactionId))
        val error = intercept[IllegalStateException] {
          session.range(2, 3).write.mode(SaveMode.Ignore).yt(tmpPath)
        }
        error.getMessage should include ("was not applied during output planning")
        readIds(tmpPath) shouldEqual Seq(0L)
        YtWrapper.exists(tmpPath, Some(transactionId)) shouldBe false
        YtWrapper.transactionExists(transactionId) shouldBe true
      }
    }
  }

  for (mode <- Seq(SaveMode.Ignore, SaveMode.ErrorIfExists)) {
    it should s"check uncommitted output existence in mode $mode" in {
      withExternalTransaction() { (session, _) =>
        session.range(1).write.yt(tmpPath)
        if (mode == SaveMode.Ignore) {
          session.range(1, 2).write.mode(mode).yt(tmpPath)
        } else {
          an [org.apache.spark.sql.AnalysisException] should be thrownBy {
            session.range(1, 2).write.mode(mode).yt(tmpPath)
          }
        }
        session.read.yt(tmpPath).collect().toSeq shouldEqual Seq(Row(0L))
      }
    }
  }

  it should "reject an unapplied V2 session transaction before setting up the write" in {
    withConf("spark.datasource.yt.write_transaction", "1-2-3-4") {
      val writeInfo = new LogicalWriteInfo {
        override def queryId(): String = "unapplied-transaction"

        override def schema(): StructType = new StructType().add("id", LongType)

        override def options(): CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()
      }
      val write = YtWrite(Seq(tmpPath), "YT", _ => true, writeInfo)
      val error = intercept[IllegalStateException] {
        write.toBatch
      }
      error.getMessage should include ("was not applied during output planning")
      YtWrapper.exists(tmpPath) shouldBe false
    }
  }

  it should "allow opting out of the session write transaction without extensions" in {
    withSparkSession(Map("spark.sql.extensions" -> "")) { baseSession =>
      withExternalTransaction(baseSession) { (session, externalTransaction) =>
        session.range(1).write.option("WRITE_TRANSACTION", "").yt(tmpPath)
        readIds(tmpPath) shouldEqual Seq(0L)
        externalTransaction.abort().join()
        readIds(tmpPath) shouldEqual Seq(0L)
      }
    }
  }

  it should "write partitioned tables under the session transaction" in {
    withExternalTransaction() { (session, _) =>
      session.range(4).selectExpr("id", "id % 2 as part").write.partitionBy("part").yt(tmpPath)
      session.read.yt(tmpPath).count() shouldBe 4
      YtWrapper.exists(tmpPath) shouldBe false
    }
  }

  it should "append with distributed writing to an uncommitted table" in {
    withSparkSession(Map("spark.yt.write.distributed.enabled" -> "true")) { baseSession =>
      withExternalTransaction(baseSession) { (session, externalTransaction) =>
        session.range(1).write.yt(tmpPath)
        session.range(1, 2).write.mode(SaveMode.Append).yt(tmpPath)
        session.read.yt(tmpPath).collect().toSeq should contain theSameElementsAs Seq(Row(0L), Row(1L))
        readIds(tmpPath, Some(externalTransaction.getId.toString)) should contain theSameElementsAs Seq(0L, 1L)
        YtWrapper.exists(tmpPath) shouldBe false
        externalTransaction.commit().join()
        readIds(tmpPath) should contain theSameElementsAs Seq(0L, 1L)
      }
    }
  }

  it should "reject external transactions for dynamic writes unless explicitly opted out" in {
    val session = spark.newSession()
    val externalTransaction = YtWrapper.createTransaction(None, Duration.ofMinutes(5))
    YtWrapper.createDynTable(tmpPath, TableSchema.builder()
      .addKey("id", ColumnValueType.INT64).addValue("value", ColumnValueType.INT64).build())
    YtWrapper.mountTableSync(tmpPath, Duration.ofMinutes(1))
    try {
      session.conf.set("spark.datasource.yt.write_transaction", externalTransaction.getId.toString)
      an [InconsistentDynamicWriteException] should be thrownBy {
        session.range(1).selectExpr("id", "id as value").write.mode(SaveMode.Append)
          .option("inconsistent_dynamic_write", "true").yt(tmpPath)
      }
      val error = intercept[InconsistentDynamicWriteException] {
        spark.range(1).selectExpr("id", "id as value").write.mode(SaveMode.Append)
          .option("inconsistent_dynamic_write", "true")
          .option("write_transaction", externalTransaction.getId.toString).yt(tmpPath)
      }
      error.getMessage should include (tmpPath)
      error.getMessage should include (externalTransaction.getId.toString)
      session.range(1).selectExpr("id", "id as value").write.mode(SaveMode.Append)
        .option("inconsistent_dynamic_write", "true").option("write_transaction", "").yt(tmpPath)
      YtWrapper.transactionExists(externalTransaction.getId.toString) shouldBe true
    } finally {
      YtWrapper.unmountTableSync(tmpPath, Duration.ofMinutes(1))
      externalTransaction.close()
    }
  }

  it should "isolate session defaults and allow explicit opt-out and overrides" in {
    val configuredSession = spark.newSession()
    val independentSession = spark.newSession()
    val defaultTransaction = YtWrapper.createTransaction(None, Duration.ofMinutes(5))
    val overrideTransaction = YtWrapper.createTransaction(None, Duration.ofMinutes(5))
    try {
      val defaultTransactionId = defaultTransaction.getId.toString
      configuredSession.conf.set("spark.datasource.yt.transaction", defaultTransactionId)
      configuredSession.conf.set("spark.datasource.yt.write_transaction", defaultTransactionId)
      configuredSession.range(1).write.yt(tmpPath)
      configuredSession.read.yt(tmpPath).collect().toSeq shouldEqual Seq(Row(0L))
      independentSession.conf.getOption("spark.datasource.yt.transaction") shouldBe None
      YtWrapper.exists(tmpPath) shouldBe false
      configuredSession.range(1, 2).write
        .option("write_transaction", overrideTransaction.getId.toString).yt(s"${tmpPath}_other")
      configuredSession.read.option("transaction", overrideTransaction.getId.toString).yt(s"${tmpPath}_other")
        .collect().toSeq shouldEqual Seq(Row(1L))
      configuredSession.range(2, 3).write.option("WRITE_TRANSACTION", "").yt(s"${tmpPath}_public")
      configuredSession.read.option("transaction", "").yt(s"${tmpPath}_public")
        .collect().toSeq shouldEqual Seq(Row(2L))
      defaultTransaction.abort().join()
      overrideTransaction.abort().join()
      YtWrapper.exists(tmpPath) shouldBe false
      YtWrapper.exists(s"${tmpPath}_other") shouldBe false
      independentSession.read.yt(s"${tmpPath}_public").collect().toSeq shouldEqual Seq(Row(2L))
    } finally {
      defaultTransaction.close()
      overrideTransaction.close()
      YtWrapper.remove(s"${tmpPath}_public", force = true)
    }
  }

  private def readIds(path: String, transactionId: Option[String] = None): Seq[Long] = {
    readTableAsYson(path, transactionId).map(_.asMap().get("id").longValue())
  }

  private def withExternalTransaction(baseSession: SparkSession = spark)
    (testBody: (SparkSession, ApiServiceTransaction) => Unit): Unit = {
    val session = baseSession.newSession()
    val externalTransaction = YtWrapper.createTransaction(None, Duration.ofMinutes(5))
    try {
      session.conf.set("spark.datasource.yt.transaction", externalTransaction.getId.toString)
      session.conf.set("spark.datasource.yt.write_transaction", externalTransaction.getId.toString)
      testBody(session, externalTransaction)
    } finally {
      externalTransaction.close()
    }
  }
}
