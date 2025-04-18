import spyt.SparkVersion.compileSparkVersion

resolvers += MavenCache("local-maven", Path.userHome / ".m2" / "repository")

addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16")

addDependencyTreePlugin

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")

addSbtPlugin("com.github.sbt" % "sbt-javaagent" % "0.1.8")

useCoursier := false

lazy val ytPublishPlugin = RootProject(file("../yt-publish-plugin"))

lazy val root = (project in file("."))
  .dependsOn(ytPublishPlugin)
  .settings(
    Compile / unmanagedSourceDirectories += file("project")
  )

// only for org.apache.spark.launcher.JavaModuleOptions class
// TODO use spark version from Dependencies
libraryDependencies += "org.apache.spark" %% "spark-launcher" % compileSparkVersion notTransitive()
