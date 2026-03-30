rootProject.name = "ytsaurus-spyt"

val defaultSpytVersion = "2.9.0-SNAPSHOT"
val customSpytVersion: String? by settings
var spytVersion = customSpytVersion ?: defaultSpytVersion
val isSnapshot = spytVersion.endsWith("-SNAPSHOT")

if (isSnapshot) {
    val uniqueSuffix = "-${System.currentTimeMillis()}${System.nanoTime() and 0x7FFFFFFFFFFFFFFFL}-SNAPSHOT"
    spytVersion = spytVersion.replace("-SNAPSHOT", uniqueSuffix)
}

val testSparkVersion: String? by settings
val adapterSparkVersions = listOf("3.2.2", "3.3.0", "3.4.0", "3.5.0").map { it to it.replace(".", "") }
val sparkBundle = listOf("spark-core", "spark-sql")

include("spark-adapter-api")
project(":spark-adapter-api").projectDir = file("spark-adapter/api")

include("spyt-patch-agent")
project(":spyt-patch-agent").projectDir = file("spark-patch")

include("spark-adapter-impl-base")
project(":spark-adapter-impl-base").projectDir = file("spark-adapter/impl/base")

adapterSparkVersions.forEach { (version, vShort) ->
    include("spark-adapter-impl-$vShort")
    project(":spark-adapter-impl-$vShort").projectDir = file("spark-adapter/impl/spark-$version")
}

include("yt-wrapper")

include("file-system")

include("data-source-base")
project(":data-source-base").projectDir = file("data-source")

include("data-source-extended")

include("resource-manager")

include("shuffle-service")

include("spyt-connect")

include("spark-cluster")

include("spark-submit")

include("spyt-test")

include("spyt-package")

val publishBlacklist = setOf("spark-adapter-impl-base", "spyt-test", "spyt-package")
val excludeScalaList = setOf("ytsaurus-spyt", "spyt-package")

gradle.allprojects {
    val scalaProject = name !in excludeScalaList
    val publishProject = name !in publishBlacklist
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

    group = "tech.ytsaurus.spyt"
}

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            fun createSparkBundle(suffix: String, versionRef: String): Unit {
                sparkBundle.forEach { libName ->
                    library("$libName$suffix", "org.apache.spark", "${libName}_2.12").versionRef(versionRef)
                }
                bundle("spark$suffix", sparkBundle.map { "$it$suffix" })
            }

            createSparkBundle("", "spark-compile")

            adapterSparkVersions.forEach { (version, vShort) ->
                val versionRef = "spark$vShort"
                version(versionRef, version)
                createSparkBundle(vShort, versionRef)
            }

            val testSparkVersionRef = if (testSparkVersion != null) {
                version("spark-test") {
                    strictly(testSparkVersion!!)
                }
                "spark-test"
            } else {
                "spark-compile"
            }
            createSparkBundle("test", testSparkVersionRef)
        }
    }
}
