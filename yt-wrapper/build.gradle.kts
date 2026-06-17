import tech.ytsaurus.spyt.gradle.tasks.BuildInfoTask

plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

val scalaVersion: String? by extra

dependencies {
    api(project(":spark-adapter-api_$scalaVersion"))
    api(libs.ytsaurus.client) {
        exclude(group = "io.netty")
        exclude(group = "com.fasterxml.jackson.core")
        exclude(group = "org.apache.commons")
        exclude(group = "com.google.code.findbugs", module = "jsr305")
        exclude(group = "org.lz4", module = "lz4-java")
    }
    api(libs.bundles.circe.scala(project))
    api(libs.log4j.layout.template.json)
    api(libs.shapeless.scala(project))
    api(libs.sttp.scala(project))

    testImplementation(libs.bundles.test.scala(project))
    testImplementation(libs.bundles.junit.platform)
    testImplementation(project(":spark-adapter-impl-330_$scalaVersion"))
    testImplementation(project(":spark-adapter-impl-340_$scalaVersion"))
    testImplementation(project(":spark-adapter-impl-350_$scalaVersion"))
    if (scalaVersion == "2.13") {
        testImplementation(project(":spark-adapter-impl-400_$scalaVersion"))
        testImplementation(project(":spark-adapter-impl-410_$scalaVersion"))
        testImplementation(project(":spark-adapter-impl-420_$scalaVersion"))
    }
    testImplementation(project(":spark-adapter-provider_$scalaVersion"))
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
