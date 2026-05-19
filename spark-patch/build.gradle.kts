dependencies {
    compileOnly(findProject(":spark-adapter-api_2.13") ?: project(":spark-adapter-api_2.12"))
}

tasks.jar {
    manifest {
        attributes("PreMain-Class" to "tech.ytsaurus.spyt.patch.SparkPatchAgent")
    }
}
