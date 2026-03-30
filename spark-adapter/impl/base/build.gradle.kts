dependencies {
    compileOnlyApi(project(":spark-adapter-api")) {
        exclude(group = "org.apache.spark")
    }
    compileOnlyApi(project(":spyt-patch-agent"))
}
