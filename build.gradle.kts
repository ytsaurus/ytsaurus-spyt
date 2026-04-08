import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters

// This prevents simultaneous execution of tests from different subprojects
abstract class TestsMutex : BuildService<BuildServiceParameters.None>

val testsMutex = gradle.sharedServices.registerIfAbsent("testsMutex", TestsMutex::class) {
    maxParallelUsages = 1
}

subprojects {
    apply(plugin = "scala")

    repositories {
        mavenCentral()
        maven {
            url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        }
    }

    configurations.create("testArtifacts") {
        extendsFrom(configurations.testImplementation.get())
    }

    tasks.register<Jar>("testJar") {
        archiveClassifier.set("tests")
        from(sourceSets["test"].output)
    }

    artifacts {
        add("testArtifacts", tasks["testJar"])
    }

    val baseJdkVersion = 11

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(baseJdkVersion)
        }
    }

    // This is a workaround for scala 2.12 compiler, which should target on 1.8 java class version
    // Don't forget to move this to spark 3.x.x adapters when the main codebase will use Scala 2.13
    tasks.withType<ScalaCompile>().configureEach {
        scalaCompileOptions.additionalParameters = listOf("-target:jvm-1.8")
    }

    val testJdkVersion = providers.gradleProperty("testJdkVersion")
        .getOrElse(baseJdkVersion.toString())
        .toInt()

    tasks.withType<Test>().configureEach {
        usesService(testsMutex)
        javaLauncher = javaToolchains.launcherFor {
            languageVersion = JavaLanguageVersion.of(testJdkVersion)
        }
        debugOptions{
            enabled = false
            host = "*"
            port = 5006
            suspend = true
        }
    }

    val isSnapshot: Boolean by extra
    val publishProject: Boolean by extra

    if (publishProject) {
        java {
            withJavadocJar()
            withSourcesJar()
        }

        val scaladoc = tasks.named<ScalaDoc>("scaladoc") {
            source = sourceSets["main"].allSource
        }

        tasks.named<Jar>("javadocJar") {
            from(scaladoc)
        }

        tasks.named<Javadoc>("javadoc") {
            enabled = false
        }

        publishing {
            publications {
                create<MavenPublication>("maven") {
                    from(components["java"])

                    pom {
                        name.set("YTsaurus SPYT")
                        description.set("YTsaurus SPYT is a tool that allows you to run Apache Spark applications on the YTsaurus platform")
                        url.set("https://github.com/ytsaurus/ytsaurus-spyt")
                        licenses {
                            license {
                                name.set("The Apache License, Version 2.0")
                                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                            }
                        }
                        developers {
                            developer {
                                id.set("YTsaurus")
                                name.set("YTsaurus SPYT development team")
                                email.set("dev@ytsaurus.tech")
                                organization.set("YTsaurus")
                                organizationUrl.set("https://ytsaurus.tech")
                            }
                        }
                        scm {
                            connection.set("scm:git:git://github.com/ytsaurus/ytsaurus-spyt.git")
                            developerConnection.set("scm:git:ssh://github.com/ytsaurus/ytsaurus-spyt.git")
                            url.set("https://github.com/ytsaurus/ytsaurus-spyt")
                        }
                    }
                }
            }

            if (!isSnapshot) {
                repositories {
                    maven {
                        url = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")

                        credentials {
                            username = project.properties["ossrhUsername"].toString()
                            password = project.properties["ossrhPassword"].toString()
                        }
                    }
                }
            }
        }

        signing {
            if (isSnapshot) {
                isRequired = false
            } else {
                isRequired = true

                val signingKey: String? by project
                val signingPassword: String? by project

                useInMemoryPgpKeys(signingKey, signingPassword)

                sign(publishing.publications["maven"])
            }
        }
    }
}
