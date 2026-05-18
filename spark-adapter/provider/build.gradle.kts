val scalaVersion: String? by extra
val adapterVersions = listOf("330", "340", "350") + if (scalaVersion == "2.13") listOf("400", "410", "420") else listOf()

dependencies {
    compileOnly(project(":spark-adapter-api_$scalaVersion"))
    adapterVersions.forEach { version ->
        compileOnly(project(":spark-adapter-impl-${version}_${scalaVersion}"))
    }
}

sourceSets.main{
    scala {
        if (scalaVersion == "2.13") {
            srcDir("src/main/scala-2.13")
        }
    }
}

tasks.register("generateResources") {
    val outputFile =
        file("${layout.buildDirectory.get()}/generated/resources/META-INF/services/tech.ytsaurus.spyt.SparkAdapterProvider")
    val implVersion = if (scalaVersion == "2.13") 4 else 3

    outputs.file(outputFile)

    doLast {
        outputFile.parentFile.mkdirs()
        outputFile.writeText("tech.ytsaurus.spyt.SparkAdapterProviderImpl$implVersion")
    }
}

tasks.processResources {
    dependsOn("generateResources")
    from("${layout.buildDirectory.get()}/generated/resources")
}

