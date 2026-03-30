plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

dependencies {
    implementation(project(":yt-wrapper"))

    testImplementation(project(mapOf("path" to ":yt-wrapper", "configuration" to "testArtifacts")))
}

sourceSets {
    main {
        java.setSrcDirs(emptyList<String>())
        scala.srcDir("src/main/java")
    }
}
