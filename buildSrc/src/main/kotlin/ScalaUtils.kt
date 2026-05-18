import org.gradle.api.Project
import org.gradle.api.artifacts.ExternalModuleDependencyBundle
import org.gradle.api.artifacts.MinimalExternalModuleDependency
import org.gradle.api.artifacts.dsl.DependencyHandler
import org.gradle.api.provider.Provider
import org.gradle.internal.extensions.core.extra

fun Provider<MinimalExternalModuleDependency>.scala(project: Project): Provider<String> = map { dep ->
    val scalaVersion = project.extra["scalaVersion"]
    dependencyNotation(dep, scalaVersion)
}

class ScalaBundle(
    val bundleProvider: Provider<ExternalModuleDependencyBundle>,
    val project: Project
)

fun Provider<ExternalModuleDependencyBundle>.scala(project: Project): ScalaBundle =
    ScalaBundle(this, project)

fun DependencyHandler.api(bundle: ScalaBundle) {
    bundle.applyTo(this, "api")
}

fun DependencyHandler.compileOnly(bundle: ScalaBundle) {
    bundle.applyTo(this, "compileOnly")
}

fun DependencyHandler.testImplementation(bundle: ScalaBundle) {
    bundle.applyTo(this, "testImplementation")
}

private fun ScalaBundle.applyTo(handler: DependencyHandler, configurationName: String) {
    val scalaVersion = project.extra["scalaVersion"]

    bundleProvider.get().forEach { dep ->
        handler.add(configurationName, dependencyNotation(dep, scalaVersion))
    }
}

private fun dependencyNotation(dep: MinimalExternalModuleDependency, scalaVersion: Any?): String {
    val version = dep.versionConstraint.requiredVersion
    val moduleName = dep.module.name.replace("_2.13", "_$scalaVersion")
    return "${dep.module.group}:${moduleName}:$version"
}
