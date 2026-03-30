package tech.ytsaurus.spyt.gradle.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.testing.Test
import org.gradle.process.CommandLineArgumentProvider

class JavaagentPlugin : Plugin<Project> {
    override fun apply(project: Project) {

        val agentConfiguration = project.configurations.create("testWithJavaAgent") {
            isCanBeResolved = true
            isCanBeConsumed = false
            description = "Dependencies for the Java Agent to be attached during tests"
        }

        project.tasks.withType(Test::class.java).configureEach {
            jvmArgumentProviders.add(JavaagentProvider(agentConfiguration))
        }
    }
}

class JavaagentProvider(
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.NONE)
    val agentFiles: FileCollection
) : CommandLineArgumentProvider {

    override fun asArguments(): Iterable<String> {
        if (agentFiles.isEmpty) {
            return emptyList()
        }

        val jar = agentFiles.singleFile
        return listOf("-javaagent:${jar.absolutePath}")
    }
}
