dependencies {
    compileOnly(findProject(":spark-adapter-api_2.13") ?: project(":spark-adapter-api_2.12"))
}
