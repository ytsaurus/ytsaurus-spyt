package org.apache.spark.deploy.rest

import org.apache.spark.deploy.DriverDescription
import org.apache.spark.deploy.YTsaurusShuffleSupport.withYtShuffleConfs
import org.apache.spark.launcher.JavaModuleOptions
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

/**
 * Patches:
 * 1. Support for java 17 extra options on submitting to inner standalone cluster in cluster mode.
 * 2. Auto-wiring the YTsaurus shuffle service confs when spark.ytsaurus.shuffle.enabled is set.
 */
@Decorate
@OriginClass("org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet")
@Applicability(from = "3.3.0")
class StandaloneSubmitRequestServletDecorators {

  @DecoratedMethod
  @Applicability(to = "4.0.0")
  private def buildDriverDescription(request: CreateSubmissionRequest): DriverDescription = {
    request.sparkProperties = withYtShuffleConfs(request.sparkProperties)
    val originalDescription = __buildDriverDescription(request)
    val javaOpts = originalDescription.command.javaOpts ++ JavaModuleOptions.defaultModuleOptions().split(" ")
    originalDescription.copy(command = originalDescription.command.copy(javaOpts = javaOpts))
  }

  @DecoratedMethod
  @Applicability(from = "4.0.0")
  private[rest] def buildDriverDescription(
    request: CreateSubmissionRequest,
    masterUrl: String,
    masterRestPort: Int): DriverDescription = {
    request.sparkProperties = withYtShuffleConfs(request.sparkProperties)
    val originalDescription = __buildDriverDescription(request, masterUrl, masterRestPort)
    val javaOpts = originalDescription.command.javaOpts ++ JavaModuleOptions.defaultModuleOptions().split(" ")
    originalDescription.copy(command = originalDescription.command.copy(javaOpts = javaOpts))
  }

  private def __buildDriverDescription(request: CreateSubmissionRequest): DriverDescription = ???
  private[rest] def __buildDriverDescription(
    request: CreateSubmissionRequest,
    masterUrl: String,
    masterRestPort: Int): DriverDescription = ???

}
