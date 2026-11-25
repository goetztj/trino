#!/usr/bin/env bash
# Runs the Hermitage tests of the LakeVilla connector (TestLakeVillaHermitage:
# concurrent SQL transactions over JDBC) against the Docker stack, for
# combinations of LakeVilla's isolation and levels. Trino is restarted for every
# combination; the test runs with Maven in a JDK container in the stack's network.
#
# usage: ./hermitage.sh [--isolation serializable,snapshot] [--levels 01,1,2,012,none]
#        (defaults: serializable and snapshot, levels 01,2,012)
#
# Requirements: ./build.sh (images and Maven build, including trino-jdbc) and a
# running stack (docker compose up -d). LV_TRINO_PROJECT and JDK_IMAGE as for
# build.sh. Trino is left running with the default configuration.
set -euo pipefail
cd "$(dirname "$0")"

docker_dir=$(pwd)
trino_root=$(cd ../../.. && pwd)
jdk_image=${JDK_IMAGE:-eclipse-temurin:24-jdk}
project=${LV_TRINO_PROJECT:-lv-trino}

isolations=serializable,snapshot
levels=01,2,012
while [ $# -gt 0 ]; do
  case "$1" in
    --isolation) isolations=$2; shift 2 ;;
    --levels) levels=$2; shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 1 ;;
  esac
done

compose() {
  docker compose -f "$docker_dir/docker-compose.yml" "$@"
}

restart_trino() {
  LAKEVILLA_LEVELS=$1 LAKEVILLA_CONFIG=$2 compose up -d --force-recreate --no-build trino > /dev/null 2>&1
  for _ in $(seq 1 90); do
    if [ "$(compose ps trino --format '{{.Health}}')" = healthy ]; then
      return
    fi
    sleep 2
  done
  echo "Trino did not start" >&2
  exit 1
}

git_dir=$(git -C "$trino_root" rev-parse --path-format=absolute --git-common-dir)
mkdir -p "$HOME/.m2"
failed=()
for isolation in ${isolations//,/ }; do
  case "$isolation" in
    serializable) config=lvconfig.conf ;;
    snapshot) config=lvconfig-snapshot.conf ;;
    *) echo "unknown isolation $isolation" >&2; exit 1 ;;
  esac
  for level in ${levels//,/ }; do
    echo "--- isolation $isolation, levels $level"
    restart_trino "$level" "$config"
    log=$(mktemp)
    if docker run --rm --network "${project}_default" \
        -u "$(id -u):$(id -g)" \
        -e HOME=/tmp -e MAVEN_USER_HOME=/m2 \
        -e LAKEVILLA_TRINO_URL=jdbc:trino://trino:8080 -e LAKEVILLA_LEVELS="$level" \
        -v "$HOME/.m2:/m2" \
        -v "$trino_root:$trino_root" -v "$git_dir:$git_dir" -w "$trino_root" \
        "$jdk_image" \
        ./mvnw -B -Dmaven.repo.local=/m2/repository -Dair.check.skip-all=true \
          -pl plugin/trino-lakevilla test -Dtest=TestLakeVillaHermitage -Dsurefire.failIfNoSpecifiedTests=false \
        > "$log" 2>&1; then
      status=passed
    else
      status=FAILED
      failed+=("$isolation/$level")
    fi
    # the report of the test (lines "=== ..." on stdout, prefixed by the test log) and failures
    grep -o '=== .*' "$log" | sed 's/^=== //' || true
    grep -E 'Tests run:|AssertionError|ERROR\]' "$log" | grep -v 'Failures: 0, Errors: 0, Skipped: 0, Time' | head -20 || true
    echo "$status"
    rm -f "$log"
  done
done

restart_trino 01 lvconfig.conf
if [ ${#failed[@]} -gt 0 ]; then
  echo "failed: ${failed[*]}"
  exit 1
fi
echo "all combinations passed"
