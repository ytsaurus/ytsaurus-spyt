#!/usr/bin/env bash

set -e

while [[ $# -gt 0 ]]; do
  case $1 in
    -m)
      Y_PYTHON_ENTRY_POINT="$2"
      export Y_PYTHON_ENTRY_POINT
      shift
      shift
      ;;
    *)
      break
      ;;
  esac
done

if [ -n "$Y_BINARY_EXECUTABLE" ]; then
  executable="$Y_BINARY_EXECUTABLE"
else
  executable="$1"
  shift
fi

echo "RUNNING PYTHON BINARY $executable using $Y_PYTHON_ENTRY_POINT entry point with arguments $@" >&2
$(realpath $executable) $@
