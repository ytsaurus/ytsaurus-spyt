PEERDIR(
    contrib/python/pyspark-client
)

SET(SPARK_VERSION 4.2.0)
SET(SPARK_DISTRIBUTIVE spark/spark-${SPARK_VERSION}-bin-hadoop3.tgz)

FROM_SANDBOX(
    13593466859
    OUT_NOAUTO
    ${SPARK_DISTRIBUTIVE}
)

RESOURCE_FILES(
    PREFIX yt/spark/spark-over-yt/spyt-package/src/main-2-11/
    ${SPARK_DISTRIBUTIVE}
)
