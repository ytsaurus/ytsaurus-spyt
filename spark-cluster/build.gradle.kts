plugins {
    id("tech.ytsaurus.spyt.common.plugin")
    id("tech.ytsaurus.spyt.javaagent.plugin")
}

val scalaVersion: String? by extra
val dataSourceExtended = ":data-source-extended_$scalaVersion"

dependencies {
    api(project(dataSourceExtended))

    if (scalaVersion == "2.13") {
        // Jetty library is shadowed in Spark. This leads to compilation errors and this dependency is needed to prevent it.
        compileOnly(libs.jetty.servlet)
    }

    implementation(project(":spyt-connect_$scalaVersion"))

    testImplementation(project(mapOf("path" to dataSourceExtended, "configuration" to "testArtifacts")))

    testWithJavaAgent(project(":spyt-patch-agent_$scalaVersion"))
}
