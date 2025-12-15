import CommonPlugin.autoImport._
import sbt._
import spyt.SparkVersion.compileSparkVersion

object Dependencies {
  val SparkCompile = config("spark-compile")
  val SparkRuntimeTest = config("spark-runtime-test")

  lazy val circeVersion = "0.12.3"
  lazy val circeYamlVersion = "0.12.0"
  lazy val shapelessVersion = "2.3.7"
  lazy val scalatestVersion = "3.1.0"
  lazy val livyVersion = "0.8.0-incubating"
  lazy val ytsaurusClientVersion = "1.2.12"
  lazy val scalatraVersion = "2.7.0"
  lazy val mockitoVersion = "1.14.4"
  lazy val arrowVersion = "0.17.1"

  lazy val testSparkVersion = System.getProperty("testSparkVersion", compileSparkVersion)

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

  def spark(sparkVersion: String, scope: Configuration = Provided) = Seq("spark-core", "spark-sql").map { module =>
    "org.apache.spark" %% module % sparkVersion % scope
  }

  def sparkTest(sparkVersion: String) = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
  )

  lazy val compileSpark = spark(compileSparkVersion, CompileProvided)

  lazy val runtimeTestSpark = spark(testSparkVersion, TestProvided)

  lazy val sparkTestDep = sparkTest(testSparkVersion)

  lazy val sparkConnect = Seq(
    "org.apache.spark" %% "spark-connect" % compileSparkVersion % Provided
  )

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

  // There libraries aren't actually required, it's just a workaround to allow IntelliJ IDEA to correctly
  // import the project because of custom dependency scopes defined in CommonPlugin (CompileProvided and TestProvided).
  lazy val intellijIdeaCompatLibraries = spark(compileSparkVersion) ++ spark(testSparkVersion) ++ Seq(
    "io.netty" % "netty-all" % "4.1.96.Final" % Provided,
    "io.netty" % "netty-transport-native-epoll" % "4.1.96.Final" % Provided,
    "io.netty" % "netty-transport-native-epoll" % "4.1.96.Final" % Provided classifier "linux-x86_64",
    "io.netty" % "netty-transport-native-epoll" % "4.1.96.Final" % Provided classifier "linux-aarch_64",
    "io.netty" % "netty-transport-native-kqueue" % "4.1.96.Final" % Provided classifier "osx-aarch_64",
    "io.netty" % "netty-transport-native-kqueue" % "4.1.96.Final" % Provided classifier "osx-x86_64",
  )
}
