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

    Seq(
      externalResolvers := Resolver.combineDefaultResolvers(resolvers.value.toVector, mavenCentral = false),
      ivyConfigurations ++= Seq(SparkCompile, SparkRuntimeTest),
      resolvers += Resolver.mavenLocal,
      resolvers += Resolver.mavenCentral,
      resolvers += ("YTsaurusSparkReleases" at "https://repo1.maven.org/maven2"),
      resolvers += ("YTsaurusSparkSnapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots/"),
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
          Some("snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots/")
        else
          Some("releases" at "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
      },
      publishMavenStyle := true,
      libraryDependencies ++= testDeps,
      Test / fork := true,
      printCompileClasspath := {
        (Compile / dependencyClasspath).value.foreach(a => println(s"${a.metadata.get(configuration.key)} - ${a.data.getAbsolutePath}"))
      },
      printTestClasspath := {
        (Test / fullClasspath).value.foreach(a => println(s"${a.metadata.get(configuration.key)} - ${a.data.getAbsolutePath}"))
      },
      Global / pgpPassphrase := gpgPassphrase.map(_.toCharArray),
      ivyConfigurations := overrideConfigs(CompileProvided, TestProvided, NewCompileInternal, NewTestInternal)(ivyConfigurations.value)
    ) ++ credentialsSettings.map(c => credentials += c)
  }
}
