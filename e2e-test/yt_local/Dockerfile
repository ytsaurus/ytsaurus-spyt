FROM ghcr.io/ytsaurus/local:dev-24.1

WORKDIR /tmp

RUN apt-get update

RUN apt-get install -y curl unzip netcat socat

RUN set -eux; \
  for VERSION in 3.9 3.11 3.12; \
  do \
    apt-get install -y python$VERSION python$VERSION-distutils; \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python$VERSION; \
    python$VERSION -m pip install ytsaurus-client==0.13.15; \
  done

COPY ./configure.sh .
RUN ./configure.sh
RUN rm /tmp/configure.sh

COPY ./start.sh /usr/bin/start-mod.sh

EXPOSE 27001-27007

ENTRYPOINT ["bash", "/usr/bin/start-mod.sh"]
