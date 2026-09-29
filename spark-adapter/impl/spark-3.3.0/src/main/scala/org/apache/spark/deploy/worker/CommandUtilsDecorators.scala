package org.apache.spark.deploy.worker

import org.apache.spark.SecurityManager
import org.apache.spark.deploy.Command
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.deploy.worker.CommandUtils$")
object CommandUtilsDecorators {

  @DecoratedMethod
  def buildProcessBuilder(
    command: Command,
    securityManager: SecurityManager,
    memory: Int,
    sparkHome: String,
    substituteArguments: String => String,
    classPaths: Seq[String],
    env: scala.collection.Map[String, String]): ProcessBuilder = {
    val builder = __buildProcessBuilder(
      command,
      securityManager,
      memory,
      sparkHome,
      substituteArguments,
      classPaths,
      env)
    builder.environment().remove("YT_SECURE_VAULT_YT_TOKEN")
    builder.environment().remove("YT_SECURE_VAULT_YT_USER")
    builder
  }

  def __buildProcessBuilder(
    command: Command,
    securityManager: SecurityManager,
    memory: Int,
    sparkHome: String,
    substituteArguments: String => String,
    classPaths: Seq[String],
    env: scala.collection.Map[String, String]): ProcessBuilder = ???
}
