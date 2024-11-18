package tech.ytsaurus.spyt.patch.annotations;

public @interface PatchSource {
    /**
     * The patch class that should be used instead of this class. It is helpful when origin class was renamed,
     * but method bodies wasn't changed, for example all methods of CastBase class for versions prior to 3.4.0
     * were moved to Cast class since 3.4.0 version.
     */
    String value();
}
