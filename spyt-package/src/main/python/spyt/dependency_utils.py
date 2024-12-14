def require_yt_client():
    try:
        import yt.wrapper  # noqa: F401
        from yt.wrapper import YtClient  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Please install ytsaurus-client (or yandex-yt for internal Yandex users). "
            "These libraries cannot be installed at the same time"
        ) from e


def require_pyspark():
    compatible_versions_str = '>=3.2.2,<4.0.0'
    try:
        import pyspark
        try:
            from packaging.specifiers import SpecifierSet
            from .version import __version__ as spyt_version
            compatible_versions = SpecifierSet(compatible_versions_str)
            if pyspark.__version__ not in compatible_versions:
                raise AssertionError(
                    f"ytsaurus-spyt {spyt_version} is compatible with pyspark{compatible_versions_str}, "
                    f"however you have pyspark {pyspark.__version__}. Please install compatible pyspark package "
                    f"or try to upgrade ytsaurus-spyt"
                )
        except ImportError:
            pass
    except ImportError as e:
        raise ImportError(
            f"Please install pyspark module within versions range {compatible_versions_str}. "
            f"It is required to use spyt."
        ) from e
