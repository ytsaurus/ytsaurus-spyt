plugins {
    id("tech.ytsaurus.spyt.common.plugin")
    id("tech.ytsaurus.spyt.javaagent.plugin")
}

dependencies {
    api(project(":spark-cluster"))
    implementation(project(":resource-manager"))

    testImplementation(project(mapOf("path" to ":spark-cluster", "configuration" to "testArtifacts")))

    testWithJavaAgent(project(":spyt-patch-agent"))
}
