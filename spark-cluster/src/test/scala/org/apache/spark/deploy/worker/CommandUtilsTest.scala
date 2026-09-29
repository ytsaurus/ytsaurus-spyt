package org.apache.spark.deploy.worker

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.Command
import org.apache.spark.util.Utils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class CommandUtilsTest extends AnyFlatSpec with Matchers {
  "CommandUtils" should "remove cluster credentials while preserving application credentials" in {
    val sparkHome = Files.createTempDirectory("spark-command-utils")
    try {
      Files.createDirectory(sparkHome.resolve("jars"))
      val environment = Map(
        "YT_SECURE_VAULT_YT_TOKEN" -> "owner-token",
        "YT_SECURE_VAULT_YT_USER" -> "owner",
        "YT_SECURE_VAULT_SPARK_APPLICATION_TOKEN" -> "application-token",
        "APPLICATION_SETTING" -> "value"
      )
      val command = Command(
        "org.apache.spark.deploy.worker.DriverWrapper",
        Seq.empty,
        environment,
        Seq.empty,
        Seq.empty,
        Seq("-Dspark.hadoop.yt.token=submitter-token"))
      val builder = CommandUtils.buildProcessBuilder(
        command,
        new SecurityManager(new SparkConf(false)),
        512,
        sparkHome.toString,
        identity[String],
        Seq.empty,
        Map.empty)

      builder.environment().containsKey("YT_SECURE_VAULT_YT_TOKEN") shouldBe false
      builder.environment().containsKey("YT_SECURE_VAULT_YT_USER") shouldBe false
      builder.environment().get("YT_SECURE_VAULT_SPARK_APPLICATION_TOKEN") shouldBe "application-token"
      builder.environment().get("APPLICATION_SETTING") shouldBe "value"
      builder.command().contains("-Dspark.hadoop.yt.token=submitter-token") shouldBe true
    } finally {
      Utils.deleteRecursively(sparkHome.toFile)
    }
  }
}
