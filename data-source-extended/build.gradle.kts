plugins {
    id("tech.ytsaurus.spyt.common.plugin")
    id("tech.ytsaurus.spyt.javaagent.plugin")
}

dependencies {
    api(project(":data-source-base"))

    testImplementation(project(mapOf("path" to ":data-source-base", "configuration" to "testArtifacts")))

    testWithJavaAgent(project(":spyt-patch-agent"))
}
