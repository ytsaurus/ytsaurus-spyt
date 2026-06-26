PY3_LIBRARY()

# TODO: Temporary solution; remove once trunk consumers migrate from
# src/main to src/main-2-11 REST submit.

SUBSCRIBER(g:spyt)

SET(SPYT_VARIANT_DIR yt/spark/spark-over-yt/spyt-package/src/main-2-11)

INCLUDE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/spark_distrib.inc)

FROM_SANDBOX(
    12569215995 AUTOUPDATED spyt_cluster
    OUT_NOAUTO spyt_cluster/spyt-package.zip
)

INCLUDE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/spyt_package_common.inc)

END()

RECURSE(bin)
