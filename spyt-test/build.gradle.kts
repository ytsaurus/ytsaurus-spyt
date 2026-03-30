plugins {
    id("tech.ytsaurus.spyt.common.plugin")
    id("tech.ytsaurus.spyt.javaagent.plugin")
}

dependencies {
    testImplementation(project(mapOf("path" to ":spark-cluster", "configuration" to "testArtifacts")))
    testImplementation(project(":shuffle-service"))

    testWithJavaAgent(project(":spyt-patch-agent"))
}
