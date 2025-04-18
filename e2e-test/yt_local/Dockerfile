FROM ghcr.io/ytsaurus/local:stable-spyt

WORKDIR /tmp

RUN apt-get update

RUN apt-get install -y curl unzip netcat socat

RUN set -eux; \
  for VERSION in 3.9 3.11 3.12; \
  do \
    apt-get install -y python$VERSION; \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python$VERSION; \
    python$VERSION -m pip install --ignore-installed ytsaurus-client==0.13.20 packaging==24.1; \
  done

COPY ./start.sh /usr/bin/start-mod.sh

EXPOSE 27001-27007

ENTRYPOINT ["bash", "/usr/bin/start-mod.sh"]
