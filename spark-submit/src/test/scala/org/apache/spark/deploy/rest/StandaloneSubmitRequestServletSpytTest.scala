package org.apache.spark.deploy.rest

import org.apache.spark.SparkConf
import org.apache.spark.deploy.DriverDescription
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.{InvocationTargetException, Method}

import scala.util.Try

class StandaloneSubmitRequestServletSpytTest extends AnyFlatSpec with Matchers {
  behavior of "StandaloneSubmitRequestServlet"

  private val masterUrl = "spark://master.address:7077"
  private val masterRestPort = 6066
  private val shuffleManagerProperty = "-Dspark.shuffle.manager="
  private val shuffleDataIoProperty = "-Dspark.shuffle.sort.io.plugin.class="

  private val shuffleManagerOpt =
    s"${shuffleManagerProperty}org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleManager"
  private val shuffleDataIoOpt =
    s"${shuffleDataIoProperty}tech.ytsaurus.spyt.shuffle.YTsaurusShuffleDataIO"

  it should "wire YTsaurus shuffle confs when spark.ytsaurus.shuffle.enabled is true" in {
    val request = submissionRequest(
      Map("spark.ytsaurus.shuffle.enabled" -> "true"))

    val description = buildDriverDescription(request)

    description.command.javaOpts should contain(shuffleManagerOpt)
    description.command.javaOpts should contain(shuffleDataIoOpt)
  }

  it should "not wire YTsaurus shuffle confs when disabled or not configured" in {
    assertYTsaurusShuffleIsNotWired(Map("spark.app.name" -> "test"))
    assertYTsaurusShuffleIsNotWired(Map("spark.ytsaurus.shuffle.enabled" -> "false"))
  }

  private def assertYTsaurusShuffleIsNotWired(
      sparkProperties: Map[String, String]): Unit = {
    val description =
      buildDriverDescription(submissionRequest(sparkProperties))

    description.command.javaOpts.exists(
      _.startsWith(shuffleManagerProperty)) shouldBe false
    description.command.javaOpts.exists(
      _.startsWith(shuffleDataIoProperty)) shouldBe false
  }

  it should "fail fast when spark properties are not set" in {
    val request = submissionRequest(null)

    val exception = intercept[InvocationTargetException] {
      buildDriverDescription(request)
    }

    exception.getCause shouldBe an[IllegalArgumentException]
    exception.getCause.getMessage should include("sparkProperties must be set")
  }

  it should "override explicitly set shuffle confs to keep the pair consistent" in {
    val request = submissionRequest(Map(
      "spark.ytsaurus.shuffle.enabled" -> "true",
      "spark.shuffle.manager" ->
        "org.apache.spark.shuffle.sort.SortShuffleManager"
    ))

    val description = buildDriverDescription(request)

    description.command.javaOpts
      .filter(_.startsWith(shuffleManagerProperty)) should contain only shuffleManagerOpt

    description.command.javaOpts
      .filter(_.startsWith(shuffleDataIoProperty)) should contain only shuffleDataIoOpt
  }

  private def buildDriverDescription(
      request: CreateSubmissionRequest): DriverDescription = {
    val servlet =
      new StandaloneSubmitRequestServlet(null, masterUrl, new SparkConf(false))

    val (method, args) = buildDriverDescriptionMethod(request)

    method.setAccessible(true)
    method.invoke(servlet, args: _*).asInstanceOf[DriverDescription]
  }

  private def buildDriverDescriptionMethod(
      request: CreateSubmissionRequest): (Method, Array[AnyRef]) = {
    val servletClass = classOf[StandaloneSubmitRequestServlet]

    Try {
      servletClass.getDeclaredMethod(
        "buildDriverDescription",
        classOf[CreateSubmissionRequest]
      ) -> Array[AnyRef](request)
    }.getOrElse {
      servletClass.getDeclaredMethod(
        "buildDriverDescription",
        classOf[CreateSubmissionRequest],
        classOf[String],
        classOf[Int]
      ) -> Array[AnyRef](request, masterUrl, Int.box(masterRestPort))
    }
  }

  private def submissionRequest(
      sparkProperties: Map[String, String]): CreateSubmissionRequest = {
    val request = new CreateSubmissionRequest
    request.appResource = "yt:///path/to/my/app.jar"
    request.mainClass = "com.example.Main"
    request.appArgs = Array.empty[String]
    request.environmentVariables = Map.empty[String, String]
    request.sparkProperties = sparkProperties
    request
  }
}
