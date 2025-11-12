#!/usr/bin/env bash

script_name=$0

cd "$(dirname "$0")"
build_output=../../../build_output
publish_scripts=../publisher
image_cr="ghcr.io/"
spark_version=""

print_usage() {
    cat <<EOF
Usage: $script_name [-h|--help]
                    [--build-output build_output]
                    [--publish-scripts publish_scripts]
                    [--spyt-version spyt_version]
                    [--spark-version spark_version]
                    [--image-cr image_cr]

  --build-output: Path to built spyt components (default: $build_output)
  --publish-scripts: Path to spyt publish scripts (default: $publish_scripts)
  --spyt-version: Spyt version
  --spark-version: Spark version (e.g., 3.5.7) to include as an archive. Requires Spark 3.4.0 or newer. (Optional)
  --image-cr: Image container registry (default: $image_cr)

EOF
    exit 0
}

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --build-output)
        build_output="$2"
        shift 2
        ;;
        --publish-scripts)
        publish_scripts="$2"
        shift 2
        ;;
        --spyt-version)
        spyt_version="$2"
        shift 2
        ;;
        --spark-version)
        spark_version="$2";
        shift 2
        ;;
        -h|--help)
        print_usage
        shift
        ;;
        *)  # unknown option
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

if [[ -z "$spyt_version" ]]; then
  echo "No version specified"
  exit 1
fi

if [[ -n "$spark_version" ]]; then
      if [[ "$spark_version" < "3.4.0" ]]; then
          echo "Error: Spark version $spark_version is not supported. Only versions 3.4.0 or higher are supported." >&2
          exit 1
      fi
    echo "Spark version to download: $spark_version"
else
    echo "Spark archive download: SKIPPED"
fi

docker_image_tag="${image_cr}ytsaurus/spyt:${spyt_version}${spark_version:+-pyspark-$spark_version}"

mkdir data
cp -rL $build_output/* data/
mkdir scripts
cp -rL $publish_scripts/* scripts/

docker build \
    --network=host \
    -t "$docker_image_tag" \
    --build-arg BUILD_OUTPUT_PATH=data \
    --build-arg PUBLISH_SCRIPTS_PATH=scripts \
    --build-arg SPARK_VERSION="$spark_version" \
    .

rm -rf data
rm -rf scripts
