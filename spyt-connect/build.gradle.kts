dependencies {
    compileOnly(project(":resource-manager"))
    compileOnly(project(":spyt-patch-agent"))
    compileOnlyApi(libs.spark.connect)
}
