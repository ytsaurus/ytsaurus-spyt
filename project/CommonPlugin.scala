import Dependencies._
import com.jsuereth.sbtpgp.PgpKeys.pgpPassphrase
import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import spyt.SpytPlugin.autoImport._
import spyt.YtPublishPlugin

object CommonPlugin extends AutoPlugin {
  override def trigger = AllRequirements

  override def requires = JvmPlugin && YtPublishPlugin

  object autoImport {
    lazy val printCompileClasspath = taskKey[Unit]("")
    lazy val printTestClasspath = taskKey[Unit]("")

    lazy val CompileProvided = config("compileprovided").describedAs("Compile time dependency that is not visible in tests and not included into distributive")
    lazy val NewCompileInternal = config("compile-internal").extend(Compile, Optional, Provided, CompileProvided)

    lazy val TestProvided = config("testprovided").describedAs("Provided test time dependency that is only used for running tests")
    lazy val NewTestInternal = config("test-internal").extend(Test, Optional, Provided, TestProvided)
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = {
    val credentialsDir = Path.userHome / ".sbt"
    val credentialsFile = credentialsDir / ".credentials"
    val ossrhCredentialsFile = credentialsDir / ".ossrh_credentials"

    val credentialsSettings = Seq(
      credentialsFile,
      ossrhCredentialsFile
    ).filter(_.exists()).map(Credentials(_))

    val consoleTesting = if (sys.env.get("SPYT_TEST_CONSOLE").exists(_.toBoolean)) {
      Seq(Test / connectInput := true)
    } else {
      Nil
    }

    Seq(
      externalResolvers := Resolver.combineDefaultResolvers(resolvers.value.toVector, mavenCentral = false),
      ivyConfigurations ++= Seq(SparkCompile, SparkRuntimeTest),
      resolvers += Resolver.mavenLocal,
      resolvers += Resolver.mavenCentral,
      resolvers += ("YTsaurusSparkReleases" at "https://central.sonatype.com/api/v1/publish"),
      resolvers += ("YTsaurusSparkSnapshots" at "https://central.sonatype.com/repository/maven-snapshots/"),
      ThisBuild / version := (ThisBuild / spytVersion).value,
      organization := "tech.ytsaurus",
      name := s"spark-yt-${name.value}",
      organizationName := "YTsaurus",
      organizationHomepage := Some(url("https://ytsaurus.tech/")),
      scmInfo := Some(ScmInfo(url("https://github.com/ytsaurus/ytsaurus"), "scm:git@github.com:ytsaurus/ytsaurus.git")),
      developers := List(
        Developer("YTsaurus", "YTsaurus SPYT development team", "dev@ytsaurus.tech", url("https://ytsaurus.tech/")),
      ),
      description := "YTsaurus SPYT",
      licenses := List(
        "The Apache License, Version 2.0" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
      ),
      homepage := Some(url("https://ytsaurus.tech/")),
      pomIncludeRepository := { _ => false },
      scalaVersion := "2.12.15",
      javacOptions ++= Seq("-source", "11", "-target", "11"),
      publishTo := {
        if (isSnapshot.value)
          Some("snapshots" at "https://central.sonatype.com/repository/maven-snapshots/")
        else
          Some("releases" at "https://central.sonatype.com/api/v1/publish")
      },
      publishMavenStyle := true,
      libraryDependencies ++= testDeps,
      Test / fork := true,
      Test / javaOptions ++= (if (sys.env.get("SPYT_TEST_REMOTE_DEBUG").exists(_.toBoolean)) {
        Seq("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5006")
      } else {
        Nil
      }),
      Test / javaOptions ++= org.apache.spark.launcher.JavaModuleOptions.defaultModuleOptions().split(' ').toSeq,
      Test / envVars ++= Map(
        "SPYT_TESTING" -> "1"
      ),
      printCompileClasspath := {
        (Compile / dependencyClasspath).value.foreach(a => println(s"${a.metadata.get(configuration.key)} - ${a.data.getAbsolutePath}"))
      },
      printTestClasspath := {
        (Test / fullClasspath).value.foreach(a => println(s"${a.metadata.get(configuration.key)} - ${a.data.getAbsolutePath}"))
      },
      Global / pgpPassphrase := gpgPassphrase.map(_.toCharArray),
      ivyConfigurations := overrideConfigs(CompileProvided, TestProvided, NewCompileInternal, NewTestInternal)(ivyConfigurations.value)
    ) ++ consoleTesting ++ credentialsSettings.map(c => credentials += c)
  }
}
