plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

val scalaVersion: String? by extra
val ytWrapper = ":yt-wrapper_$scalaVersion"

dependencies {
    api(project(ytWrapper))

    testImplementation(project(mapOf("path" to ytWrapper, "configuration" to "testArtifacts")))
    if (scalaVersion == "2.12") {
        testImplementation("${libs.spark.coretest212.get()}:tests")
    } else {
        testImplementation("${libs.spark.coretest.get()}:tests")
    }
}
