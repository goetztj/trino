#!/usr/bin/env bash
# Builds the Docker image of Trino with the LakeVilla connector:
#
#   1. builds Trino (trino-server-core, the lakevilla, tpch and delta-lake
#      plugins and the CLI) with Maven in a JDK container (no local JDK needed; the Maven
#      repository ~/.m2 is reused)
#   2. copies the artifacts to dist/
#   3. builds the images of docker-compose.yml; the trino image compiles
#      liblakevilla.so from the LakeVilla repository (LAKEVILLA_REPO)
#
# usage: ./build.sh [--skip-maven] [--skip-images]
#
# Environment:
#   LAKEVILLA_REPO   checkout of the LakeVilla repository
#                    (default: ../../../../LakeVilla, next to the Trino repository)
#   MAVEN_ARGS       additional Maven arguments (e.g. -o)
#   JDK_IMAGE        image of the Maven build (default eclipse-temurin:24-jdk)
set -euo pipefail
cd "$(dirname "$0")"

docker_dir=$(pwd)
trino_root=$(cd ../../.. && pwd)
export LAKEVILLA_REPO=${LAKEVILLA_REPO:-$(cd ../../../.. && pwd)/LakeVilla}
jdk_image=${JDK_IMAGE:-eclipse-temurin:24-jdk}

skip_maven=false
skip_images=false
for arg in "$@"; do
  case "$arg" in
    --skip-maven) skip_maven=true ;;
    --skip-images) skip_images=true ;;
    *) echo "unknown argument $arg" >&2; exit 1 ;;
  esac
done

if [ ! -f "$LAKEVILLA_REPO/LV/src/LakeVilla/TransactionManager/jni_trino.cpp" ]; then
  echo "LAKEVILLA_REPO=$LAKEVILLA_REPO is not a LakeVilla checkout with the Trino JNI binding (LV/src/LakeVilla/TransactionManager/jni_trino.cpp)" >&2
  exit 1
fi

version=$(sed -n 's:.*<version>\(.*-SNAPSHOT\)</version>.*:\1:p' "$trino_root/pom.xml" | head -1)
echo "Trino $version, LakeVilla $LAKEVILLA_REPO"

if [ "$skip_maven" = false ]; then
  mkdir -p "$HOME/.m2"
  # the modules of trino-server-core are not dependencies of it: list them explicitly
  modules=core/trino-server-main,core/trino-server-core,client/trino-cli,plugin/trino-tpch,plugin/trino-lakevilla
  # Trino's Delta Lake connector: interoperability tests (catalog delta)
  modules+=,plugin/trino-delta-lake
  modules+=,plugin/trino-exchange-filesystem,plugin/trino-functions-python,plugin/trino-geospatial
  modules+=,plugin/trino-password-authenticators,plugin/trino-resource-group-managers
  modules+=,plugin/trino-session-property-managers,plugin/trino-spooling-filesystem
  # the sources are mounted at their own path: in a git worktree, .git refers to the
  # repository's git directory by its absolute path (git-commit-id plugin)
  git_dir=$(git -C "$trino_root" rev-parse --path-format=absolute --git-common-dir)
  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -e HOME=/tmp -e MAVEN_USER_HOME=/m2 \
    -v "$HOME/.m2:/m2" \
    -v "$trino_root:$trino_root" -v "$git_dir:$git_dir" -w "$trino_root" \
    "$jdk_image" \
    ./mvnw -B -Dmaven.repo.local=/m2/repository -DskipTests -Dair.check.skip-all=true -T 1C \
      ${MAVEN_ARGS:-} -pl "$modules" -am install
fi

rm -rf dist
mkdir -p dist/plugin
cp "$trino_root/core/trino-server-core/target/trino-server-core-$version.tar.gz" dist/
cp "$trino_root/client/trino-cli/target/trino-cli-$version-executable.jar" dist/trino-cli
for plugin in tpch lakevilla delta-lake; do
  mkdir -p "dist/plugin/$plugin"
  unzip -q "$trino_root/plugin/trino-$plugin/target/trino-$plugin-$version.zip" -d "dist/plugin/tmp-$plugin"
  mv "dist/plugin/tmp-$plugin"/*/* "dist/plugin/$plugin/"
  rm -rf "dist/plugin/tmp-$plugin"
done

if [ "$skip_images" = false ]; then
  docker compose -f "$docker_dir/docker-compose.yml" build --build-arg TRINO_VERSION="$version"
fi
echo "done: docker compose -f $docker_dir/docker-compose.yml up -d"
