dependencies {
    compileOnlyApi(project(":spark-adapter-api_2.13")) {
        exclude(group = "org.apache.spark")
    }
    compileOnlyApi(project(":spyt-patch-agent_2.13"))
    compileOnly(libs.bundles.spark410)
}
