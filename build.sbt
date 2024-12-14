import Dependencies._
import com.jsuereth.sbtpgp.PgpKeys.publishSigned
import sbt.io.Path.relativeTo
import spyt.PythonPlugin.autoImport._
import spyt.SparkPaths._
import spyt.SpytPlugin.autoImport._
import spyt.YtPublishPlugin.autoImport._

lazy val `spark-adapter-api` = (project in file("spark-adapter/api"))
  .settings(
    libraryDependencies ++= defaultSpark,
  )

lazy val `spark-patch` = (project in file("spark-patch"))
  .dependsOn(`spark-adapter-api` % "compile->compile;test->test;provided->provided")
  .settings(
    libraryDependencies ++= livy,
    Compile / packageBin / packageOptions +=
      Package.ManifestAttributes(new java.util.jar.Attributes.Name("PreMain-Class") -> "tech.ytsaurus.spyt.patch.SparkPatchAgent")
  )

lazy val javaAgents = Def.task {
  Seq(JavaAgent.ResolvedAgent(
    JavaAgent.AgentModule("spark-patch", null, JavaAgent.AgentScope(test = true, dist = false), ""),
    (`spark-patch` / Compile / packageBin).value
  ))
}


lazy val `spark-adapter-impl-322` = (project in file(s"spark-adapter/impl/spark-3.2.2"))
  .dependsOn(`spark-adapter-api`, `spark-patch` % Provided)
  .settings(
    libraryDependencies ++= spark("3.2.2")
  )

lazy val `spark-adapter-impl-330` = (project in file(s"spark-adapter/impl/spark-3.3.0"))
  .dependsOn(`spark-adapter-api`, `spark-patch` % Provided)
  .settings(
    libraryDependencies ++= spark("3.3.0")
  )

lazy val `spark-adapter-impl-340` = (project in file(s"spark-adapter/impl/spark-3.4.0"))
  .dependsOn(`spark-adapter-api`, `spark-patch` % Provided)
  .settings(
    libraryDependencies ++= spark("3.4.0")
  )

lazy val `yt-wrapper` = (project in file("yt-wrapper"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(`spark-adapter-api` % "compile->compile;test->test;provided->provided")
  .settings(
    libraryDependencies ++= circe,
    libraryDependencies ++= shapeless,
    libraryDependencies ++= sttp,
    libraryDependencies ++= ytsaurusClient,
    libraryDependencies ++= logging,
    libraryDependencies ++= testDeps,
    buildInfoKeys := Seq[BuildInfoKey](version, BuildInfoKey.constant(("ytClientVersion", ytsaurusClientVersion))),
    buildInfoPackage := "tech.ytsaurus.spyt"
  )

lazy val `file-system` = (project in file("file-system"))
  .enablePlugins(CommonPlugin)
  .dependsOn(`yt-wrapper` % "compile->compile;test->test;provided->provided")

lazy val `data-source-base` = (project in file("data-source"))
  .dependsOn(
    `file-system` % "compile->compile;test->test;provided->provided",
    `spark-adapter-impl-322` % Test,
    `spark-adapter-impl-330` % Test,
    `spark-adapter-impl-340` % Test
  )

lazy val `data-source-extended` = (project in file("data-source-extended"))
  .dependsOn(`data-source-base` % "compile->compile;test->test;provided->provided")
  .enablePlugins(JavaAgent)
  .settings(
    resolvedJavaAgents := javaAgents.value
  )

lazy val `resource-manager` = (project in file("resource-manager"))
  .dependsOn(`yt-wrapper` % "compile->compile;test->test;provided->provided")
  .settings(
    libraryDependencies ++= defaultSparkTest
  )

lazy val `cluster` = (project in file("spark-cluster"))
  .dependsOn(`data-source-extended` % "compile->compile;test->test;provided->provided")
  .enablePlugins(JavaAgent)
  .settings(
    libraryDependencies ++= scaldingArgs,
    libraryDependencies ++= scalatra,
    resolvedJavaAgents := javaAgents.value
  )

lazy val `spark-submit` = (project in file("spark-submit"))
  .dependsOn(`cluster` % "compile->compile;test->test;provided->provided", `resource-manager` % "compile->compile;test->test")
  .enablePlugins(JavaAgent)
  .settings(
    libraryDependencies ++= scaldingArgs,
    resolvedJavaAgents := javaAgents.value
  )

val snapshotPaths =
  """
    |spark.ytsaurus.config.releases.path                   //home/spark/conf/snapshots
    |spark.ytsaurus.spyt.releases.path                     //home/spark/spyt/snapshots
    |""".stripMargin

lazy val `spyt-package` = (project in file("spyt-package"))
  .enablePlugins(JavaAppPackaging, PythonPlugin)
  .dependsOn(
    `spark-submit` % "compile->compile;test->test;provided->provided",
    `spark-patch`,
    `spark-adapter-impl-322`,
    `spark-adapter-impl-330`,
    `spark-adapter-impl-340`
  )
  .settings(

    // These dependencies are already provided by spark distributive
    excludeDependencies ++= Seq(
      "commons-lang" % "commons-lang",
      "org.apache.commons" % "commons-lang3",
      "org.typelevel" %% "cats-kernel",
      "org.lz4" % "lz4-java",
      "io.dropwizard.metrics" % "metrics-core",
      "org.slf4j" % "slf4j-api",
      "org.scala-lang.modules" %% "scala-parser-combinators",
      "org.scala-lang.modules" %% "scala-xml",
    ),
    excludeDependencies ++= Seq(
      ExclusionRule(organization = "org.apache.httpcomponents")
    ),

    Compile / discoveredMainClasses := Seq(),
    Universal / packageName := "spyt-package",
    Universal / mappings ++= {
      val dir = sourceDirectory.value / "main" / "spark-extra"
      (dir ** AllPassFilter --- dir) pair relativeTo(dir)
    },
    Universal / mappings ++= {
      val dir = sourceDirectory.value / "main" / "python" / "spyt"
      val pythonFilter = new SimpleFileFilter(file =>
        !(file.getName.contains("__pycache__") || file.getName.endsWith(".pyc"))
      )
      (dir ** pythonFilter --- dir) pair relativeTo(dir.getParentFile.getParentFile)
    },
    Universal / mappings := {
      val oldMappings = (Universal / mappings).value
      val scalaLibs = scalaInstance.value.libraryJars.toSet
      oldMappings.filterNot(lib => scalaLibs.contains(lib._1)).map { case (file, targetPath) =>
        file -> (if (targetPath.startsWith("lib/")) s"jars/${targetPath.substring(4)}" else targetPath)
      }
    },

    setupSpytEnvScript := sourceDirectory.value / "main" / "bash" / "setup-spyt-env.sh",

    pythonDeps := {
      val packagePaths = (Universal / mappings).value.flatMap {
        case (file, path) if !file.isDirectory =>
          val target = path.substring(0, path.lastIndexOf("/")) match {
            case dir@("jars"|"bin"|"conf") => s"spyt/$dir"
            case dir@("python/spyt") => "spyt"
            case x => x
          }
          Some(target -> file)
        case _ => None
      }
      val binBasePath = sourceDirectory.value / "main" / "bin"
      binBasePath.listFiles().map(f => "bin" -> f) ++ packagePaths
    },
    pythonAppends := {
      if (isSnapshot.value) { Seq(
        "spyt/conf/spark-defaults.conf" -> snapshotPaths,
        "spyt/conf/log4j.properties" -> "log4j.rootLogger=INFO, console\n"
      ) } else {
        Seq.empty
      }
    }
  )
  .settings(
    spytArtifacts := {
      val rootDirectory = baseDirectory.value.getParentFile
      val files = Seq((Universal / packageBin).value, setupSpytEnvScript.value)
      makeLinksToBuildDirectory(files, rootDirectory)
      val versionValue = (ThisBuild / spytVersion).value
      val baseConfigDir = (Compile / resourceDirectory).value
      val logger = streams.value.log
      if (configGenerationEnabled) {
        val sidecarConfigsFiles = if (innerSidecarConfigEnabled) {
          spyt.ClusterConfig.innerSidecarConfigs(baseConfigDir)
        } else {
          spyt.ClusterConfig.sidecarConfigs(baseConfigDir)
        }
        copySidecarConfigsToBuildDirectory(rootDirectory, sidecarConfigsFiles)
        val launchConfigYson = spyt.ClusterConfig.launchConfig(versionValue, sidecarConfigsFiles)
        dumpYsonToConfBuildDirectory(launchConfigYson, rootDirectory, "spark-launch-conf")
        val globalConfigYsons = spyt.ClusterConfig.globalConfig(logger, versionValue, baseConfigDir)
        globalConfigYsons.foreach {
          case (proxy, config) => dumpYsonToConfBuildDirectory(config, rootDirectory, s"global-$proxy")
        }
      } else {
        copySidecarConfigsToBuildDirectory(rootDirectory,
          spyt.ClusterConfig.innerSidecarConfigs(baseConfigDir), "inner-sidecar-config")
        copySidecarConfigsToBuildDirectory(rootDirectory,
          spyt.ClusterConfig.sidecarConfigs(baseConfigDir), "sidecar-config")
      }
    },
    publishYtArtifacts ++= {
      val versionValue = (ThisBuild / spytVersion).value
      val isSnapshotValue = isSnapshotVersion(versionValue)
      val isTtlLimited = isSnapshotValue && limitTtlEnabled

      val basePath = versionPath(spytPath, versionValue)

      val clusterConfigArtifacts = spyt.ClusterConfig.artifacts(streams.value.log, versionValue,
        (Compile / resourceDirectory).value).map {
        case ytPublishFile: YtPublishFile =>
          if (ytPublishFile.localFile.getName == "livy-client.template.conf" && isSnapshot.value) {
            ytPublishFile.copy(append = ("\n" + snapshotPaths.trim.linesIterator.map(_.replaceAll("\\s+", " = ")).mkString("\n")).getBytes)
          } else {
            ytPublishFile
          }
        case other => other
      }

      val spytPackageZip = getBuildDirectory(baseDirectory.value.getParentFile) / s"${(Universal / packageName).value}.zip"

      Seq(
        YtPublishFile(spytPackageZip, basePath, None, isTtlLimited = isTtlLimited),
        YtPublishFile(setupSpytEnvScript.value, basePath, None, isTtlLimited = isTtlLimited, isExecutable = true)
      ) ++ clusterConfigArtifacts
    }
  )


lazy val root = (project in file("."))
  .enablePlugins(SpytPlugin)
  .aggregate(
    `spark-adapter-api`,
    `spark-adapter-impl-322`,
    `spark-adapter-impl-330`,
    `spark-adapter-impl-340`,
    `yt-wrapper`,
    `file-system`,
    `data-source-base`,
    `data-source-extended`,
    `cluster`,
    `resource-manager`,
    `spark-submit`,
    `spark-patch`,
    `spyt-package`
  )
  .settings(
    prepareBuildDirectory := {
      streams.value.log.info(s"Preparing build directory in ${baseDirectory.value}")
      deleteBuildDirectory(baseDirectory.value)
    },

    spytDistributive := Def.taskDyn {
      val spytPackageArchives = `spyt-package` / spytArtifacts
      val pythonWhl = Def.task {
          val pythonDist = (`spyt-package` / pythonBuild).value
          makeLinkToBuildDirectory(pythonDist, baseDirectory.value, "ytsaurus-spyt")
      }
      Def.sequential(spytPackageArchives, pythonWhl)
    }.value,

    publishToYt := {
      if (publishYtEnabled) {
        (`spyt-package` / publishYt).value
      } else {
        streams.value.log.info("Publishing SPYT files to YT is skipped because no cluster was specified")
      }
    },

    publishToPypi := {
      (`spyt-package` / pythonUpload).value
    },

    publishToMavenCentral := {
      Def.sequential(
        `spark-adapter-api` / publishSigned,
        `spark-adapter-impl-322` / publishSigned,
        `spark-adapter-impl-330` / publishSigned,
        `spark-adapter-impl-340` / publishSigned,
        `yt-wrapper` / publishSigned,
        `file-system` / publishSigned,
        `spark-patch` / publishSigned,
        `data-source-base` / publishSigned,
        `data-source-extended` / publishSigned,
        `resource-manager` / publishSigned,
        `cluster` / publishSigned,
        `spark-submit` / publishSigned
      ).value
    }
  )
