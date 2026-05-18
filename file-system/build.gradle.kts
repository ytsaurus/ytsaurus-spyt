plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

val ytWrapper = ":yt-wrapper_${extra["scalaVersion"]}"

dependencies {
    api(project(ytWrapper))

    testImplementation(project(mapOf("path" to ytWrapper, "configuration" to "testArtifacts")))
}
