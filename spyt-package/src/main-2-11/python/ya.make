PY3_LIBRARY()

SUBSCRIBER(g:spyt)

COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/__init__.py spyt/__init__.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/_submit_common.py spyt/_submit_common.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/arcadia.py spyt/arcadia.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/client.py spyt/client.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/common.py spyt/common.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/conf.py spyt/conf.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/connect.py spyt/connect.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/dependency_utils.py spyt/dependency_utils.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/direct_submit.py spyt/direct_submit.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/enabler.py spyt/enabler.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/extensions.py spyt/extensions.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/jvm.py spyt/jvm.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/launch_utils.py spyt/launch_utils.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/spec.py spyt/spec.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/standalone.py spyt/standalone.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/task_proxy.py spyt/task_proxy.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/types.py spyt/types.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/utils.py spyt/utils.py)
COPY_FILE(${ARCADIA_ROOT}/yt/spark/spark-over-yt/spyt-package/src/main/python/spyt/version.py spyt/version.py)

PY_SRCS(
    TOP_LEVEL
    spyt/__init__.py
    spyt/_submit_common.py
    spyt/_variant.py
    spyt/arcadia.py
    spyt/client.py
    spyt/common.py
    spyt/conf.py
    spyt/connect.py
    spyt/dependency_utils.py
    spyt/direct_submit.py
    spyt/enabler.py
    spyt/extensions.py
    spyt/jvm.py
    spyt/launch_utils.py
    spyt/spec.py
    spyt/standalone.py
    spyt/submit.py
    spyt/task_proxy.py
    spyt/types.py
    spyt/utils.py
    spyt/version.py
)

END()
