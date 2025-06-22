#!/bin/bash

set -ex

# Source common functions
source "$(dirname "$0")/common.sh"

set_default_vars

main "$@"
