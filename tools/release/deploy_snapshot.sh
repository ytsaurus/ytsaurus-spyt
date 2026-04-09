#!/usr/bin/env bash

set -ex

script_name=$0
os_release=false

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--proxy <proxy address>]
                    [--os-release]

  --proxy:       YT proxy address
  --os-release:  use OS release mode (by default inner release mode is enabled.

EOF
    exit 0
}

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --proxy)
        proxy="$2"
        shift 2
        ;;
        --os-release)
        os_release=true
        shift
        ;;
        -h|--help)
        print_usage
        ;;
        *)  # unknown option
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

if [[ -z "$proxy" ]]; then
  echo "No YT proxy address specified"
  exit 1
fi
export YT_PROXY=$proxy

script_dir="$(dirname "$0")"
build_dir="$(realpath $script_dir/build)"
artifacts_dir="$script_dir/../../spyt-package/build/output"

mv "$artifacts_dir" "$build_dir"
trap 'rm -r $build_dir' EXIT

cd "$script_dir"

config_generator_args=(build)
if [[ "$os_release" != true ]]; then
    config_generator_args+=(--inner-release)
fi

python3 -m publisher.config_generator "${config_generator_args[@]}"
python3 -m publisher.publish_cluster build --ignore-existing
