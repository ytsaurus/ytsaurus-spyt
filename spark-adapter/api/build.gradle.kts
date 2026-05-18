val scalaVersion: String? by extra

dependencies {
    if (scalaVersion == "2.12") {
        compileOnlyApi(libs.bundles.spark212)
    } else {
        compileOnlyApi(libs.bundles.spark)
    }
}
