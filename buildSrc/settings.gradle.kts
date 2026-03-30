dependencyResolutionManagement {
    versionCatalogs {
        create("projectLibs") {
            from(files("../gradle/libs.versions.toml"))
        }
    }
}
