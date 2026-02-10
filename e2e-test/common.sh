#!/bin/bash

# Common functions and variables for test runners

set_default_vars() {
    spyt_version="2.999.0"
    cur_dir=$(pwd)

    # Open root
    cd ..
    root_dir=$(pwd)

    rebuild="true"
    start_yt_local="true"
    deploy="true"
    run_tox_tests="true"
    extra_config_generator_options=""
    extra_tox_params=""

    yt_local_runner_path="$root_dir/../../docker/local/run_local_cluster.sh"
    proxy_port=8000
    spark_cache_path=""
    spark_versions="3.2.2 3.2.4 3.3.4 3.4.4 3.5.7"
    spark_versions_to_install=""
    versions_combinations=""
}

print_usage() {
    script_name=$(basename "$0")
    cat <<EOF
Runner for Python tests. It requires preinstalled Python 3.9, 3.11, 3.12, tox >= 4.0.0, docker.

Usage: $script_name [-h|--help]
                    [--no-rebuild]
                    [--reuse-yt]
                    [--no-deploy]
                    [--no-tests]
                    [--yt-local-runner-path path]
                    [--spark-cache-path path]
                    [--proxy-port]
                    [tox-args...]

  --no-rebuild: Skip rebuilding SPYT artifacts
  --reuse-yt: Don't start local YTsaurus and use existing one
  --no-deploy: Skip deploying SPYT artifacts to YTsaurus cluster
  --no-tests: Don't run tox tests
  --yt-local-runner-path: Path to local YTsaurus run script
  --spark-cache-path: Local directory with downloaded Spark distributives
  --proxy-port: Port for local YT
  -e: Combinations of python and spark versions for parallel launch tests in Arcanum

EOF
}

main() {
    spark_from_env() {
        local env_name="$1"
        local base_env="${env_name%%-extraspark*}"
        local raw="${base_env##*-spark}"
        echo "${raw:0:1}.${raw:1:1}.${raw:2:1}"
    }

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
            --no-tests)
            run_tox_tests="false"
            shift
            ;;
            --yt-local-runner-path)
            yt_local_runner_path="$2"
            shift 2
            ;;
            --proxy-port)
            proxy_port="$2"
            shift 2
            ;;
            --spark-cache-path)
            spark_cache_path="$2"
            shift 2
            ;;
            -e) # ex: -e py311-spark322,py312-spark354,py312-spark355
            versions_combinations="$2"
            spark_versions_to_install=""
            extraspark_digits=""
            IFS=',' read -ra envs <<< "$versions_combinations"
            for env in "${envs[@]}"; do
                [[ "$env" == *"extraspark"* ]] && extraspark_digits="${env##*extraspark}"
                spark_versions_to_install+="$(spark_from_env "$env") "
            done
            if [ -n "$extraspark_digits" ]; then
                spark_versions_to_install+="${extraspark_digits:0:1}.${extraspark_digits:1:1}.${extraspark_digits:2:1} "
            fi
            spark_versions_to_install=$(echo "$spark_versions_to_install" | xargs)
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

    # Save remaining arguments
    REMAINING_ARGS=("$@")

    if [ "$rebuild" = "true" ]; then
      # Build SPYT artifacts
      sbt -DcustomSpytVersion=$spyt_version spytBuildRelease
      # Download pre-built Livy distributive
      wget -nv -nc -P $root_dir/build_output https://storage.yandexcloud.net/ytsaurus-spyt/livy.tgz
      # Build SPYT docker image
      cd $root_dir/tools/release/spyt_image
      ./build.sh --spyt-version $spyt_version
    fi;

    export PROXY_PORT=$proxy_port

    if [ "$start_yt_local" = "true" ]; then
        echo "START_YT_LOCAL"
        cd $root_dir/e2e-test
        # Run YTsaurus local with 3 nodes
        $yt_local_runner_path \
          --rpc-proxy-count 1 \
          --node-count 3 \
          --proxy-port $proxy_port \
          --interface-port $((proxy_port + 1)) \
          --rpc-proxy-port $((proxy_port + 2)) \
          --yt-version spyt-testing \
          --yt-skip-pull true \
          --extra-yt-docker-opts "-p 27001-27150:27001-27150 --env PROXY_PORT=$proxy_port"
        trap "echo 'Stopping YT local' && $yt_local_runner_path --stop" EXIT
        sleep 5 # waiting for nodes to start
    fi

    if [ "$deploy" = "true" ]; then
        spark_cache_mount=""
        if [ "$spark_cache_path" ]; then
          spark_cache_mount="-v $spark_cache_path:$spark_cache_path"
        fi

        # Build docker run command
        versions_to_deploy="${spark_versions_to_install:-$spark_versions}"
        docker_cmd="docker run --network=host \
                   -e YT_PROXY=\"localhost:$proxy_port\" -e YT_USER=\"root\" -e YT_TOKEN=\"token\" \
                   -e SPARK_DISTRIB_USE_CACHE=\"true\" \
                   -e SPARK_DISTRIB_VERSIONS=\"$versions_to_deploy\" \
                   -v /tmp:/tmp \
                   $spark_cache_mount"

        # Add extra config options if present
        if [ -n "$spark_cache_path" ]; then
            docker_cmd="$docker_cmd -e SPARK_DISTRIB_CACHE_PATH=\"$spark_cache_path\""
        fi
        if [ -n "$extra_config_generator_options" ]; then
            docker_cmd="$docker_cmd -e EXTRA_CONFIG_GENERATOR_OPTIONS=\"$extra_config_generator_options\""
        fi

        # Complete the command
        docker_cmd="$docker_cmd --rm ghcr.io/ytsaurus/spyt:$spyt_version"

        # Execute the docker command
        eval $docker_cmd
    fi

    if [ "$run_tox_tests" = "true" ]; then
        spark_part="not two_spark"
        [[ "$versions_combinations" == *"extraspark"* ]] && spark_part="two_spark"
        if [ -z "${PYTHON_FILTER:-}" ]; then
            PYTHON_FILTER='-m "'
        else
            PYTHON_FILTER="${PYTHON_FILTER} and "
        fi
        PYTHON_FILTER="${PYTHON_FILTER}${spark_part}\""
        export PYTHON_FILTER="$PYTHON_FILTER"

        export SPARK_VERSIONS="${spark_versions_to_install:-$spark_versions}"
        cd "$cur_dir"
        tox_command="tox"

        if [ -n "$versions_combinations" ]; then
            tox_command="$tox_command -e $versions_combinations"
        fi

        if [ ${#REMAINING_ARGS[@]} -eq 0 ]; then
            tox_command="$tox_command -- tests -v"
        else
            tox_command="$tox_command ${REMAINING_ARGS[@]}"
        fi

        eval $tox_command
    fi
}
