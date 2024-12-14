#!/bin/bash

set -ex

spyt_version="2.999.0"
cur_dir=$(pwd)

# Open root
cd ..
root_dir=$(pwd)

rebuild="true"
start_yt_local="true"
yt_local_runner_path="$root_dir/../../docker/local/run_local_cluster.sh"
spark_cache_path=""
deploy="true"

print_usage() {
    cat <<EOF
Runner for Python tests. It requires preinstalled Python 3.9, 3.11, 3.12, tox >= 4.0.0, docker.

Usage: $script_name [-h|--help]
                    [--no-rebuild]
                    [--reuse-yt]
                    [--no-deploy]
                    [--yt-local-runner-path path]
                    [--spark-cache-path path]
                    [tox-args...]

  --no-rebuild: Skip rebuilding SPYT artifacts
  --reuse-yt: Don't start local YTsaurus and use existing one
  --no-deploy: Skip deploying SPYT artifacts to YTsaurus cluster
  --yt-local-runner-path: Path to local YTsaurus run script
  --spark-cache-path: Local directory with downloaded Spark distributives

EOF
    exit 0
}

# Parsing arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --no-rebuild)
        rebuild="false"
        shift
        ;;
        --reuse-yt)
        start_yt_local="false"
        shift
        ;;
        --no-deploy)
        deploy="false"
        shift
        ;;
        --yt-local-runner-path)
        yt_local_runner_path="$2"
        shift 2
        ;;
        --spark-cache-path)
        spark_cache_path="$2"
        shift 2
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
    # Build SPYT artifacts
    sbt -DcustomSpytVersion=$spyt_version spytBuildRelease
fi;

if [ "$start_yt_local" = "true" ]; then
    # Run YTsaurus local with 3 nodes
    $yt_local_runner_path --rpc-proxy-count 1 --node-count 3 --proxy-port 8000 --yt-version spyt-testing \
                          --yt-skip-pull true --extra-yt-docker-opts "-p 27001-27150:27001-27150"
    trap "echo 'Stopping YT local' && $yt_local_runner_path --stop" EXIT
fi;

if [ "$deploy" = "true" ]; then
    # Download pre-built Livy distributive
    wget -nc -P $root_dir/build_output https://storage.yandexcloud.net/ytsaurus-spyt/livy.tgz

    # Build SPYT docker image
    cd $root_dir/tools/release/spyt_image
    ./build.sh --spyt-version $spyt_version

    spark_cache_parameter=""
    spark_cache_mount=""
    if [ "$spark_cache_path" ]; then
      spark_cache_parameter="--cache-path ${spark_cache_path}"
      spark_cache_mount="-v $spark_cache_path:$spark_cache_path"
    fi;
    # Deploy with SPYT image
    docker run --network=host \
               -e YT_PROXY="localhost:8000" -e YT_USER="root" -e YT_TOKEN="token" \
               -e EXTRA_SPARK_VERSIONS="--use-cache $spark_cache_parameter 3.2.2 3.2.4 3.3.0 3.3.4 3.4.0 3.4.4 3.5.0 3.5.3" \
               -v /tmp:/tmp \
               $spark_cache_mount \
               --rm \
               ghcr.io/ytsaurus/spyt:$spyt_version
fi;
# TODO change 3.5.3 to 3.5.4

# Run tests
cd $cur_dir
tox $@
