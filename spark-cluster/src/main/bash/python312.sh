#!/bin/bash

set -ex
apt-get update

PYTHON_VERSION="python3.12"

apt-get install -y software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update --allow-unauthenticated
apt-get install -y build-essential manpages-dev libpcap-dev libpq-dev libssl-dev libffi-dev
apt-get install -y $PYTHON_VERSION $PYTHON_VERSION-dev $PYTHON_VERSION-venv
curl -sS https://bootstrap.pypa.io/get-pip.py | $PYTHON_VERSION
$PYTHON_VERSION -m pip install --upgrade pip
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple yandex-yt==0.14.1
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple numpy==1.26.4
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple pyarrow==15.0.2
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple pandas==2.2.2
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple scipy==1.13.0
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple packaging==24.1

mkdir -p /opt/$PYTHON_VERSION/bin
ln -s /usr/bin/$PYTHON_VERSION /opt/$PYTHON_VERSION/bin/python
ln -s /usr/bin/$PYTHON_VERSION /opt/$PYTHON_VERSION/bin/$PYTHON_VERSION
