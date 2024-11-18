import sbt._

object Dependencies {
  lazy val circeVersion = "0.12.3"
  lazy val circeYamlVersion = "0.12.0"
  lazy val shapelessVersion = "2.3.7"
  lazy val scalatestVersion = "3.1.0"
  lazy val livyVersion = "0.8.0-incubating"
  lazy val ytsaurusClientVersion = "1.2.6"
  lazy val scalatraVersion = "2.7.0"
  lazy val mockitoVersion = "1.14.4"
  lazy val arrowVersion = "0.17.1"

  lazy val defaultSparkVersion = System.getProperty("sparkVersion", "3.2.2")

  lazy val circe = ("io.circe" %% "circe-yaml" % circeYamlVersion) +: Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)

  lazy val shapeless = Seq(
    "com.chuusai" %% "shapeless" % shapelessVersion
  )

  lazy val mockito = Seq(
    "org.mockito" %% "mockito-scala-scalatest" % mockitoVersion % Test,
    "org.mockito" %% "mockito-scala" % mockitoVersion % Test
  )

  lazy val testDeps = Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.3" % Test,
    "org.scalactic" %% "scalactic" % scalatestVersion % Test,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    "org.scalatestplus" %% "scalacheck-1-14" % "3.1.0.0" % Test
  ) ++ mockito

  def spark(sparkVersion: String) = Seq("spark-core", "spark-sql").flatMap { module => Seq(
    "org.apache.spark" %% module % sparkVersion % Provided
  )}

  def sparkTest(sparkVersion: String) = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
  )

  lazy val defaultSpark = spark(defaultSparkVersion)

  lazy val defaultSparkTest = sparkTest(defaultSparkVersion)

  lazy val ytsaurusClient = Seq(
    "tech.ytsaurus" % "ytsaurus-client" % ytsaurusClientVersion excludeAll(
    ExclusionRule(organization = "io.netty"),
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "org.apache.commons"),
    ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  ))

  lazy val scaldingArgs = Seq(
    "com.twitter" %% "scalding-args" % "0.17.4"
  )

  lazy val logging = Seq(
    "net.logstash.log4j" % "jsonevent-layout" % "1.7"
  )

  lazy val scalatra = Seq(
    "org.scalatra" %% "scalatra" % scalatraVersion,
    "org.eclipse.jetty" % "jetty-webapp" % "9.2.19.v20160908" % Compile,
    "javax.servlet" % "javax.servlet-api" % "3.1.0" % Provided
  )

  lazy val sttp = Seq(
    "com.softwaremill.sttp.client" %% "core" % "2.1.4"
  )

  lazy val livy = Seq(
    "org.apache.livy" % "livy-assembly" % livyVersion % Provided excludeAll(
      ExclusionRule(organization = "org.json4s"),
      ExclusionRule(organization = "org.scala-lang.modules"),
      ExclusionRule(organization = "com.fasterxml.jackson.module"),
    )
  )
}
