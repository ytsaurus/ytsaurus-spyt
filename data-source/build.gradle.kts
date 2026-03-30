plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

dependencies {
    api(project(":file-system"))

    testImplementation(project(mapOf("path" to ":file-system", "configuration" to "testArtifacts")))
}

sourceSets {
    main {
        java.setSrcDirs(emptyList<String>())
        scala.srcDir("src/main/java")
    }
}
