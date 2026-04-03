import tech.ytsaurus.spyt.gradle.tasks.BuildInfoTask

plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

dependencies {
    api(project(":spark-adapter-api"))
    api(libs.ytsaurus.client) {
        exclude(group = "io.netty")
        exclude(group = "com.fasterxml.jackson.core")
        exclude(group = "org.apache.commons")
        exclude(group = "com.google.code.findbugs", module = "jsr305")
    }
    api(libs.bundles.circe)
    api(libs.jsonevent.layout)
    api(libs.shapeless)
    api(libs.sttp)

    testImplementation(libs.bundles.test)
    testImplementation(libs.bundles.junit.platform)
    testImplementation(libs.scala.collection.compat)
    testImplementation(project(":spark-adapter-impl-322"))
    testImplementation(project(":spark-adapter-impl-330"))
    testImplementation(project(":spark-adapter-impl-340"))
    testImplementation(project(":spark-adapter-impl-350"))
    testImplementation(project(":spark-adapter-provider"))
}

val generateBuildInfo = tasks.register<BuildInfoTask>("generateBuildInfo") {
    description = "Generate additional source files before compilation"
    pkg = "tech.ytsaurus.spyt"
    generatedSrcDir = layout.buildDirectory.dir("generated/src/main/scala")
    versions.put("version", version.toString())
    versions.put("ytClientVersion", libs.versions.ytsaurus.client)
}

sourceSets {
    main {
        scala.srcDir(generateBuildInfo)
    }
}

tasks.compileScala {
    dependsOn("generateBuildInfo")
}
