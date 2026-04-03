import tech.ytsaurus.spyt.gradle.tasks.ProcessPythonFilesTask

dependencies {
    runtimeOnly(project(":spark-submit"))
    runtimeOnly(project(":shuffle-service"))
    runtimeOnly(project(":spyt-patch-agent"))
    runtimeOnly(project(":spark-adapter-impl-322"))
    runtimeOnly(project(":spark-adapter-impl-330"))
    runtimeOnly(project(":spark-adapter-impl-340"))
    runtimeOnly(project(":spark-adapter-impl-350"))
    runtimeOnly(project(":spark-adapter-provider"))
}

configurations.configureEach {
    exclude(group = "commons-lang", module = "commons-lang")
    exclude(group = "io.dropwizard.metrics", module = "metrics-core")
    exclude(group = "org.apache.commons", module = "commons-lang3")
    exclude(group = "org.lz4", module = "lz4-java")
    exclude(group = "org.scala-lang")
    exclude(group = "org.scala-lang.modules")
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.typelevel", module = "cats-kernel_2.12")
}

tasks {
    val prepareArtifacts = register<Sync>("prepareArtifacts") {
        group = "build"
        description = "Prepare artifacts for assembling zip archive and whl distributive"

        val artifactPrefixMap = project.provider {
            configurations.runtimeClasspath.get()
                .resolvedConfiguration
                .resolvedArtifacts
                .associate { artifact ->
                    val id = artifact.moduleVersion.id
                    artifact.file to id.group
                }
        }

        into(layout.buildDirectory.dir("intermediates/spyt-package"))
        from(configurations.runtimeClasspath) {
            into("jars")

            eachFile {
                val prefix = artifactPrefixMap.get()[this.file]
                if (prefix != null) {
                    this.name = "${prefix}-${this.sourceName}"
                }
            }
        }

        from(file("src/main/spark-extra"))
        from(file("src/main/python/spyt")) {
            into("python/spyt")
        }
    }

    val processPythonFiles = register<ProcessPythonFilesTask>("processPythonFiles") {
        description = "Preparing python wheel package"
        spytScalaVersion = version as String
        artifactsPath = prepareArtifacts.get().destinationDir
        dependsOn(prepareArtifacts)
    }

    val spytDistributive = register<Zip>("spytDistributive") {
        group = "build"
        description = "Assemble spyt-package.zip file"
        dependsOn(processPythonFiles)

        from(prepareArtifacts) {
            into("spyt-package")
        }
        useFileSystemPermissions()

        archiveBaseName.set("spyt-package")
        archiveVersion.unsetConvention()
    }

    val prepareWheel = register<Sync>("prepareWheel") {
        group = "build"
        description = "Preparing python wheel package"
        dependsOn(processPythonFiles)

        val outputDir = layout.buildDirectory.dir("wheel")
        into(outputDir)
        from(prepareArtifacts) {
            exclude("python")
            into("deps/spyt")
        }
        from(prepareArtifacts.get().destinationDir.toPath().resolve("python/spyt")) {
            into("deps/spyt")
        }
        from(file("src/main/bin")) {
            into("deps/bin")
        }
        from(file("src/main/python")) {
            exclude("spyt")
        }
    }

    val buildWheel = register<Exec>("buildWheel") {
        group = "build"
        description = "Assembling python wheel package"
        dependsOn(prepareWheel)

        workingDir(prepareWheel.get().destinationDir)
        commandLine("python3", "-m", "build")
    }

    val buildOutput = register<Sync>("buildOutput") {
        into(layout.buildDirectory.dir("output"))

        from(spytDistributive)
        from(file("src/main/bash/setup-spyt-env.sh"))
        from(prepareArtifacts.get().destinationDir.toPath().resolve("conf/version.json"))
        from(file("src/main/resources")) {
            into("conf")
        }
    }

    assemble {
        dependsOn(buildOutput)
        dependsOn(buildWheel)
    }
}
