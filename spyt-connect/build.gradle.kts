val scalaVersion: String? by extra

dependencies {
    compileOnly(project(":resource-manager_$scalaVersion"))
    compileOnly(project(":spyt-patch-agent"))
    if (scalaVersion == "2.12") {
        compileOnlyApi(libs.spark.connect212)
    } else {
        compileOnlyApi(libs.spark.connect)
    }
}
