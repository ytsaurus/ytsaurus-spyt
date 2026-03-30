package tech.ytsaurus.spyt.gradle.tasks

import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

abstract class BuildInfoTask : DefaultTask() {

    override fun getGroup(): String? {
        return "code generation"
    }

    @get:Input
    abstract val pkg: Property<String>

    @get:Input
    abstract val versions: MapProperty<String, String>

    @get:OutputDirectory
    abstract val generatedSrcDir: DirectoryProperty

    @TaskAction
    fun generateBuildInfo() {
        val outputDir = generatedSrcDir.asFile.get()

        val pkg = pkg.get()
        val packageDir = outputDir.resolve(pkg.replace(".", "/"))
        packageDir.mkdirs()

        val file = packageDir.resolve("BuildInfo.scala")
        val keys = versions.keySet().get().toList()
        val variables = keys.map {key ->
            val value = versions.getting(key).get()
            "val $key: String = \"$value\""
        }.joinToString(separator = "; ")

        file.writeText(
            """
            package $pkg

            import scala.Predef._

            case object BuildInfo {
              $variables
              override val toString: String = {
                "${keys.map {"$it: %s"}.joinToString(separator = ", ")}".format(
                  ${keys.joinToString(separator = ", ")}
                )
              }
            }
            """.trimIndent()
        )
    }
}
