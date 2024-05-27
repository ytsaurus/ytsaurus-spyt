#!/bin/bash

script_name=$0

rebuild="true"

print_usage() {
    cat <<EOF
Runner for Python unit tests. It requires preinstalled Python 3.9, 3.11, 3.12 and tox >= 4.0.0.

Usage: $script_name [-h|--help]
                    [--no-rebuild]
                    [tox-args...]

  --no-rebuild: Skip rebuilding SPYT artifacts

EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --no-rebuild)
        rebuild="false"
        shift
        ;;
        -h|--help)
        print_usage
        ;;
        *)
        break
        ;;
    esac
done

if [ "$rebuild" = "true" ]; then
  cur_dir=$(pwd)
  cd ../../../..
  sbt -DpublishRepo=False -DpublishMavenCentral=False -DpublishYt=False spytPublishSnapshot
  cd "$cur_dir"
fi;

tox $@
