plugins {
    id("tech.ytsaurus.spyt.common.plugin")
}

val scalaVersion: String? by extra

dependencies {
    compileOnly(project(":resource-manager_$scalaVersion"))
    compileOnly(project(":spyt-patch-agent"))
    testImplementation(libs.scalatest.scala(project))
    testImplementation(libs.scalatestplus.junit.scala(project))
    if (scalaVersion == "2.12") {
        compileOnlyApi(libs.spark.connect212)
        testRuntimeOnly(libs.spark.connect212)
    } else {
        compileOnlyApi(libs.spark.connect)
        testRuntimeOnly(libs.spark.connect)
    }
}
