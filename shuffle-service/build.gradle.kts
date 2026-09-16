plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

val ytWrapper = findProject(":yt-wrapper_2.13") ?: project(":yt-wrapper_2.12")
val testScalaVersion = providers.gradleProperty("testScalaVersion").getOrElse("2.13")

dependencies {
    compileOnly(ytWrapper)

    testImplementation(project(":yt-wrapper_$testScalaVersion"))
    testImplementation(project(mapOf("path" to ":yt-wrapper_$testScalaVersion", "configuration" to "testArtifacts")))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform {
        setIncludeEngines(setOf("junit-jupiter"))
    }
}
