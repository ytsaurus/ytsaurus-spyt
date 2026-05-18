plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

val ytWrapper = ":yt-wrapper_${extra["scalaVersion"]}"

dependencies {
    implementation(project(ytWrapper))

    testImplementation(project(mapOf("path" to ytWrapper, "configuration" to "testArtifacts")))
}

sourceSets {
    main {
        java.setSrcDirs(emptyList<String>())
        scala.srcDir("src/main/java")
    }
}
