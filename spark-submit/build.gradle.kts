plugins {
    id("tech.ytsaurus.spyt.common.plugin")
    id("tech.ytsaurus.spyt.javaagent.plugin")
}

val sparkCluster = ":spark-cluster_${extra["scalaVersion"]}"
val resourceManager = ":resource-manager_${extra["scalaVersion"]}"
val spytPatchAgent = ":spyt-patch-agent_${extra["scalaVersion"]}"

dependencies {
    api(project(sparkCluster))
    implementation(project(resourceManager))

    testImplementation(project(mapOf("path" to sparkCluster, "configuration" to "testArtifacts")))

    testWithJavaAgent(project(spytPatchAgent))
}
