FROM mirror.gcr.io/ubuntu:focal

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
  python3.7 \
  python3-pip \
  python3.7-distutils

RUN python3.7 -m pip install ytsaurus-client==0.13.7

COPY ${BUILD_OUTPUT_PATH}/version.json /data/
COPY ${BUILD_OUTPUT_PATH}/conf /data/conf
COPY ${BUILD_OUTPUT_PATH}/spyt-package.zip /data/
COPY ${BUILD_OUTPUT_PATH}/setup-spyt-env.sh /data/
COPY ${BUILD_OUTPUT_PATH}/livy.tgz /data/

COPY ${PUBLISH_SCRIPTS_PATH} /scripts
COPY ./entrypoint.sh /entrypoint.sh

RUN chmod 755 /entrypoint.sh

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
