plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(projectLibs.spark.launcher)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

gradlePlugin {
    plugins {
        create("spytCommonPlugin") {
            id = "tech.ytsaurus.spyt.common.plugin"
            implementationClass = "tech.ytsaurus.spyt.gradle.plugin.SpytCommonPlugin"
        }
        create("javaAgentPlugin") {
            id = "tech.ytsaurus.spyt.javaagent.plugin"
            implementationClass = "tech.ytsaurus.spyt.gradle.plugin.JavaagentPlugin"
        }
    }
}
