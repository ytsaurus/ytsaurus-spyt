rootProject.name = "ytsaurus-spyt"

val defaultSpytVersion = "2.10.0-SNAPSHOT"
val customSpytVersion: String? by settings
var spytVersion = customSpytVersion ?: defaultSpytVersion
val isSnapshot = spytVersion.endsWith("-SNAPSHOT")

if (isSnapshot) {
    val uniqueSuffix = "-${System.currentTimeMillis()}${System.nanoTime() and 0x7FFFFFFFFFFFFFFFL}-SNAPSHOT"
    spytVersion = spytVersion.replace("-SNAPSHOT", uniqueSuffix)
}

val testSparkVersion: String? by settings
val testScalaVersion: String? by settings
val adapterSparkVersions = listOf("3.3.0", "3.4.0", "3.5.0", "4.0.0", "4.1.0", "4.2.0").map {
    it to it.replace(".", "")
}
val sparkBundle = listOf("spark-core", "spark-sql")

val subprojects = mapOf(
    "spark-adapter-api" to "spark-adapter/api",
    "spark-adapter-provider" to "spark-adapter/provider",
    "data-source-base" to "data-source"
) + adapterSparkVersions.map { (version, vShort) ->
    "spark-adapter-impl-$vShort" to "spark-adapter/impl/spark-$version"
} + listOf(
    "yt-wrapper",
    "file-system",
    "data-source-extended",
    "resource-manager",
    "shuffle-service",
    "spyt-connect",
    "spark-cluster",
    "spark-submit",
    "spyt-test"
).map { it to it }

val scalaVersions = if (testScalaVersion != null) listOf(testScalaVersion!!) else listOf("2.13", "2.12")
gradle.extra["scalaVersions"] = scalaVersions

scalaVersions.forEach { scalaVersion ->
    subprojects.forEach { (baseName, projectPath) ->
        if (scalaVersion == "2.13" || !baseName.startsWith("spark-adapter-impl-4")) {
            val subprojectName = ":${baseName}_$scalaVersion"
            include(subprojectName)
            project(subprojectName).projectDir = file(projectPath)
        }
    }
}

include("spyt-patch-agent")
project(":spyt-patch-agent").projectDir = file("spark-patch")

include("spyt-package")

val publishBlacklist = setOf("spyt-test", "spyt-package")
val excludeScalaList = setOf("ytsaurus-spyt", "spyt-package")

gradle.allprojects {
    val scalaVersion = scalaVersions.find { name.endsWith("_$it") }
    val baseName = if (scalaVersion != null) name.substringBefore("_$scalaVersion") else name

    val scalaProject = baseName !in excludeScalaList
    val publishProject = baseName !in publishBlacklist
    apply(plugin = "java-library")
    if (scalaProject) {
        apply(plugin = "scala")
    }
    if (publishProject) {
        apply(plugin = "signing")
        apply(plugin = "maven-publish")
    }

    version = spytVersion
    extra["isSnapshot"] = isSnapshot
    extra["publishProject"] = publishProject
    if (scalaVersion != null) {
        extra["scalaVersion"] = scalaVersion
        layout.buildDirectory = layout.buildDirectory.get().dir("scala-$scalaVersion")
    }

    group = "tech.ytsaurus.spyt"
}

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            fun createSparkBundle(suffix: String, versionRef: String, scalaVersion: String = "2.13"): Unit {
                sparkBundle.forEach { libName ->
                    library("$libName$suffix", "org.apache.spark", "${libName}_${scalaVersion}").versionRef(versionRef)
                }
                bundle("spark$suffix", sparkBundle.map { "$it$suffix" })
            }

            createSparkBundle("", "spark-compile")
            createSparkBundle("212", "spark-compile212", "2.12")

            adapterSparkVersions.forEach { (sparkVersion, sparkVersionShort) ->
                val versionRef = "spark$sparkVersionShort"
                // TODO: temporarily use 4.2.0-preview4, remove before merge
                val updatedVersion = if (sparkVersion == "4.2.0") "4.2.0-preview4" else sparkVersion
                version(versionRef, updatedVersion)
                createSparkBundle(sparkVersionShort, versionRef)
            }

            val (testSparkVersionRef, testSparkVersionRef212) = if (testSparkVersion != null) {
                version("spark-test") {
                    strictly(testSparkVersion!!)
                }
                "spark-test" to "spark-test"
            } else {
                "spark-compile" to "spark-compile212"
            }

            createSparkBundle("test", testSparkVersionRef)
            createSparkBundle("test212", testSparkVersionRef212, "2.12")
        }
    }
}
