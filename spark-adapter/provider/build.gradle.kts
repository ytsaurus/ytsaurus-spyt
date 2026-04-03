dependencies {
    compileOnly(project(":spark-adapter-api"))
    listOf("322", "330", "340", "350").forEach { version ->
        compileOnly(project(":spark-adapter-impl-$version"))
    }
}