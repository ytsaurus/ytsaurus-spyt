plugins {
    id("tech.ytsaurus.spyt.common.plugin")
    id("tech.ytsaurus.spyt.javaagent.plugin")
}

val dataSourceBase = ":data-source-base_${extra["scalaVersion"]}"
val spytPatchAgent = ":spyt-patch-agent_${extra["scalaVersion"]}"

dependencies {
    api(project(dataSourceBase))

    testImplementation(project(mapOf("path" to dataSourceBase, "configuration" to "testArtifacts")))

    testWithJavaAgent(project(spytPatchAgent))
}
