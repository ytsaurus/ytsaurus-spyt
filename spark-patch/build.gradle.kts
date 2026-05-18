val scalaVersion: String? by extra

dependencies {
    compileOnly(project(":spark-adapter-api_${scalaVersion}"))
}

tasks.jar {
    manifest {
        attributes("PreMain-Class" to "tech.ytsaurus.spyt.patch.SparkPatchAgent")
    }
}
