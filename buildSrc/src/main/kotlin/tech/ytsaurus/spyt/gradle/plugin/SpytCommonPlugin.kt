package tech.ytsaurus.spyt.gradle.plugin


import org.gradle.api.Project
import org.gradle.api.Plugin
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.tasks.testing.Test

class SpytCommonPlugin: Plugin<Project> {
    override fun apply(project: Project) {
        project.tasks.withType(Test::class.java).configureEach {
            useJUnitPlatform {
                includeEngines("scalatest")
                val testTag = project.findProperty("testTag")
                if (testTag != null) {
                    includeTags(testTag.toString())
                }
            }
            testLogging {
                events("passed", "skipped", "failed", "standard_error", "standard_out")
            }
            jvmArgs(org.apache.spark.launcher.JavaModuleOptions.defaultModuleOptions().split(" "))
            maxHeapSize = "4g"
            environment("SPYT_TESTING", "1")
        }

        val catalogs = project.extensions.getByType(VersionCatalogsExtension::class.java)
        val libs = catalogs.named("libs")
        val sparktestBundleProvider = libs.findBundle("sparktest").get()
        project.dependencies.add("testImplementation", sparktestBundleProvider)
    }
}
