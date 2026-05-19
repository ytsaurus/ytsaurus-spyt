val scalaVersion: String? by extra

dependencies {
    compileOnlyApi(project(":spark-adapter-api_$scalaVersion")) {
        exclude(group = "org.apache.spark")
    }
    compileOnlyApi(project(":spyt-patch-agent"))
    compileOnly(libs.bundles.spark340.scala(project))
}
