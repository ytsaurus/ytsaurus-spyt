dependencies {
    compileOnly(project(":spark-adapter-api"))
    compileOnly(libs.livy) {
        exclude(group = "org.json4s")
        exclude(group = "org.scala-lang.modules")
        exclude(group = "com.fasterxml.jackson.module")
    }
}

tasks.jar {
    manifest {
        attributes("PreMain-Class" to "tech.ytsaurus.spyt.patch.SparkPatchAgent")
    }
}
