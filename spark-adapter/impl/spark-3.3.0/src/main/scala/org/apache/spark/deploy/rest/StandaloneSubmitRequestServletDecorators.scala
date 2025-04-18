package org.apache.spark.deploy.rest

import org.apache.spark.deploy.DriverDescription
import org.apache.spark.launcher.JavaModuleOptions
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

/**
 * Support for java 17 extra options on submitting to inner standalone cluster in cluster mode.
 */
@Decorate
@OriginClass("org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet")
@Applicability(from = "3.3.0")
class StandaloneSubmitRequestServletDecorators {

  @DecoratedMethod
  private def buildDriverDescription(request: CreateSubmissionRequest): DriverDescription = {
    val originalDescription = __buildDriverDescription(request)
    val javaOpts = originalDescription.command.javaOpts ++ JavaModuleOptions.defaultModuleOptions().split(" ")
    originalDescription.copy(command = originalDescription.command.copy(javaOpts = javaOpts))
  }

  private def __buildDriverDescription(request: CreateSubmissionRequest): DriverDescription = ???
}
