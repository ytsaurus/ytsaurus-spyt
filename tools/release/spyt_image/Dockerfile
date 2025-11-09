FROM mirror.gcr.io/ubuntu:noble AS base

USER root

ARG BUILD_OUTPUT_PATH
ARG PUBLISH_SCRIPTS_PATH

RUN mkdir /data

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  curl \
  less \
  gdb \
  lsof \
  strace \
  telnet \
  dnsutils \
  iputils-ping \
  lsb-release \
  openjdk-11-jdk \
  python3.12 \
  python3-pip

RUN rm /usr/lib/python3.12/EXTERNALLY-MANAGED
RUN python3.12 -m pip install requests ytsaurus-client==0.13.35

FROM base AS spark-downloader

ARG SPARK_VERSION

# only supports 3.4.0 <= version < 4.0
RUN \
  mkdir -p /spark_cache && \
  if [ -n "${SPARK_VERSION}" ]; then \
    SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_FILE}" && \
    curl -L --progress-bar "${SPARK_URL}" -o "/spark_cache/${SPARK_FILE}" && \
    CONNECT_FILE="spark-connect_2.12-${SPARK_VERSION}.jar" && \
    CONNECT_URL="https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/${SPARK_VERSION}/${CONNECT_FILE}" && \
    curl -L --progress-bar "${CONNECT_URL}" -o "/spark_cache/${CONNECT_FILE}"; \
  fi

FROM base AS final

COPY --from=spark-downloader /spark_cache/ /tmp/

COPY ${BUILD_OUTPUT_PATH}/version.json /data/
COPY ${BUILD_OUTPUT_PATH}/conf /data/conf
COPY ${BUILD_OUTPUT_PATH}/spyt-package.zip /data/
COPY ${BUILD_OUTPUT_PATH}/setup-spyt-env.sh /data/
COPY ${BUILD_OUTPUT_PATH}/livy.tgz /data/

COPY ${PUBLISH_SCRIPTS_PATH} /scripts
COPY ./entrypoint.sh /entrypoint.sh

RUN chmod 755 /entrypoint.sh

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
