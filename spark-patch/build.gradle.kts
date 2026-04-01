dependencies {
    compileOnly(project(":spark-adapter-api"))
}

tasks.jar {
    manifest {
        attributes("PreMain-Class" to "tech.ytsaurus.spyt.patch.SparkPatchAgent")
    }
}
