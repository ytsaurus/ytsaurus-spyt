plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

dependencies {
    api(project(":yt-wrapper"))

    testImplementation(project(mapOf("path" to ":yt-wrapper", "configuration" to "testArtifacts")))
    testImplementation("${libs.spark.coretest.get()}:tests")
}
