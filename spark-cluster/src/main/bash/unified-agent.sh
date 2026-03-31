#!/bin/bash

set -ex

KEYS='B8E34ED180EF6310 FF5F4D0E27393420'
for key in $KEYS; do
  curl -sL "http://keyserver.ubuntu.com/pks/lookup?op=get&search=0x${key}" | \
    gpg --dearmor -o "/etc/apt/trusted.gpg.d/${key}.gpg"
done

apt-get update
apt-get install libc6
apt-get install -t stable -y yandex-unified-agent
apt-get clean
