plugins {
    id("tech.ytsaurus.spyt.common.plugin")
    id("tech.ytsaurus.spyt.javaagent.plugin")
}

dependencies {
    api(project(":data-source-extended"))

    implementation(project(":spyt-connect"))

    testImplementation(project(mapOf("path" to ":data-source-extended", "configuration" to "testArtifacts")))

    testWithJavaAgent(project(":spyt-patch-agent"))
}
