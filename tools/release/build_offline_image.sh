#!/usr/bin/env bash
#
# Build a local offline SPYT docker image (SPYT + baked Spark/pyspark) from a
# spark-over-yt / ytsaurus-spyt checkout:
#   gradle assemble (JDK17) -> livy.tgz if Dockerfile needs it -> spyt_image/build.sh
#
# Usage: build_offline_image.sh --spyt-version X.Y.Z --spark-version A.B.C
#          [--repo DIR] [--checkout] [--java-home PATH] [--image-cr CR] [--push]
#          [--verify] [--tox-env ENV] [--proxy-port PORT] [--yt-runner PATH]
#
# Result tag: <image-cr>ytsaurus/spyt:X.Y.Z-pyspark-A.B.C
#
# --verify builds the local-YT test image (e2e-test/yt_local) and runs the tox
# e2e suite against the freshly built image via e2e-test/run-tests.sh.
# Requires: tox on PATH, JDKs at /opt/jdk11 & /opt/jdk17, and the local-YT
# runner. Inside arcadia the runner resolves by default; from a standalone
# ytsaurus-spyt clone pass --yt-runner /path/to/run_local_cluster.sh.

set -euo pipefail
die() { echo "ERROR: $*" >&2; exit 1; }

# Default repo = two levels up from this script (tools/release/ -> repo root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAVA_HOME_JDK17="${JAVA_HOME_JDK17:-/usr/lib/jvm/java-17-openjdk-amd64}"
IMAGE_CR="ghcr.io/"
DO_CHECKOUT=0 DO_PUSH=0 DO_VERIFY=0 SPYT_VERSION="" SPARK_VERSION="" TOX_ENV="" PROXY_PORT="" YT_RUNNER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --spyt-version)  SPYT_VERSION="$2"; shift 2 ;;
    --spark-version) SPARK_VERSION="$2"; shift 2 ;;
    --repo)          REPO_DIR="$2"; shift 2 ;;
    --java-home)     JAVA_HOME_JDK17="$2"; shift 2 ;;
    --image-cr)      IMAGE_CR="$2"; shift 2 ;;
    --tox-env)       TOX_ENV="$2"; shift 2 ;;
    --proxy-port)    PROXY_PORT="$2"; shift 2 ;;
    --yt-runner)     YT_RUNNER="$2"; shift 2 ;;
    --checkout)      DO_CHECKOUT=1; shift ;;
    --push)          DO_PUSH=1; shift ;;
    --verify)        DO_VERIFY=1; shift ;;
    *) die "unknown argument: $1 (see header for usage)" ;;
  esac
done

[[ -n "$SPYT_VERSION" && -n "$SPARK_VERSION" ]] || die "--spyt-version and --spark-version are required"
[[ -x "$REPO_DIR/gradlew" ]] || die "no gradlew in $REPO_DIR (not a SPYT checkout?)"
[[ -x "$JAVA_HOME_JDK17/bin/java" ]] || die "JDK17 not found at $JAVA_HOME_JDK17 (--java-home)"
docker info >/dev/null 2>&1 || die "docker not available (not installed or daemon not running)"

cd "$REPO_DIR"
TAG="${IMAGE_CR}ytsaurus/spyt:${SPYT_VERSION}-pyspark-${SPARK_VERSION}"
BUILD_OUTPUT="spyt-package/build/output"
IMG_DIR="tools/release/spyt_image"

if [[ "$DO_CHECKOUT" -eq 1 ]]; then
  # only tracked changes get clobbered by checkout; untracked build junk is fine
  if ! git diff --quiet || ! git diff --cached --quiet; then
    die "tracked changes present; commit/stash before --checkout"
  fi
  git fetch --tags origin
  git checkout "tags/spyt/$SPYT_VERSION"
fi

JAVA_HOME="$JAVA_HOME_JDK17" ./gradlew assemble -PcustomSpytVersion="$SPYT_VERSION"

# livy.tgz is not produced by gradle; some Dockerfiles COPY it
if grep -q 'livy\.tgz' "$IMG_DIR/Dockerfile"; then
  wget -nv -nc -P "$BUILD_OUTPUT" https://storage.yandexcloud.net/ytsaurus-spyt/livy.tgz
fi

# build.sh does mkdir without -p; leftovers from a failed run break it
rm -rf "$IMG_DIR/data" "$IMG_DIR/scripts"
# --image-cr only exists in build.sh since ~2.9.2; its default is ghcr.io/ too,
# so only forward it when overridden (keeps older tags like 2.9.0 working)
cr_opt=()
[[ "$IMAGE_CR" != "ghcr.io/" ]] && cr_opt=(--image-cr "$IMAGE_CR")
( cd "$IMG_DIR" && ./build.sh --spyt-version "$SPYT_VERSION" \
    --spark-version "$SPARK_VERSION" "${cr_opt[@]}" )

# build.sh ends with rm -rf, masking docker build failures behind exit 0
docker image inspect "$TAG" >/dev/null 2>&1 || die "image $TAG missing — docker build failed"
echo "built: $TAG"

if [[ "$DO_VERIFY" -eq 1 ]]; then
  # local-YT cluster image the tox e2e suite deploys onto
  ( cd e2e-test/yt_local && ./build.sh )
  # run-tests.sh deploys ghcr.io/ytsaurus/spyt:<common.sh spyt_version> — alias ours to it
  deploy_ver="$(grep -oP 'spyt_version="\K[^"]+' e2e-test/common.sh | head -1)"
  docker tag "$TAG" "${IMAGE_CR}ytsaurus/spyt:${deploy_ver}"
  # tox env from spark version: 3.5.8 -> py312-spark358-java17 (override via --tox-env)
  tox_env="${TOX_ENV:-py312-spark${SPARK_VERSION//./}-java17}"
  runner_opt=()
  [[ -n "$YT_RUNNER" ]] && runner_opt=(--yt-local-runner-path "$YT_RUNNER")
  port_opt=()  # unset -> run-tests.sh uses its standard default (8000)
  [[ -n "$PROXY_PORT" ]] && port_opt=(--proxy-port "$PROXY_PORT")
  ( cd e2e-test && ./run-tests.sh --no-rebuild "${runner_opt[@]}" "${port_opt[@]}" -e "$tox_env" )
fi

if [[ "$DO_PUSH" -eq 1 ]]; then
  docker push "$TAG"
else
  echo "to publish: docker push $TAG"
fi
