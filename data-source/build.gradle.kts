plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

val fileSystem = ":file-system_${extra["scalaVersion"]}"

dependencies {
    api(project(fileSystem))

    testImplementation(project(mapOf("path" to fileSystem, "configuration" to "testArtifacts")))
}

sourceSets {
    main {
        java.setSrcDirs(emptyList<String>())
        scala.srcDir("src/main/java")
    }
}
