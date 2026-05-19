plugins {
    id("tech.ytsaurus.spyt.common.plugin")
    id("tech.ytsaurus.spyt.javaagent.plugin")
}

val scalaVersion: String? by extra

dependencies {
    testImplementation(project(mapOf("path" to ":spark-cluster_$scalaVersion", "configuration" to "testArtifacts")))
    testImplementation(project(":shuffle-service_$scalaVersion"))

    testWithJavaAgent(project(":spyt-patch-agent"))
}
