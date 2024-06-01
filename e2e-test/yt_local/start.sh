#!/usr/bin/env bash

(socat TCP-LISTEN:8000,fork TCP:localhost:80 &) && echo "Activated redirect from :8000 to :80"

source /usr/bin/start.sh
