# syntax=docker/dockerfile:1
#
# Trino with the LakeVilla connector.
#
# Build contexts:
#   .            this directory (plugin/trino-lakevilla/docker); dist/ holds the
#                Trino artifacts copied by build.sh (server, plugins, CLI)
#   lakevilla    the LV directory of the LakeVilla repository
#                (docker compose: additional_contexts, see docker-compose.yml)
#
# Stages:
#   lv-deps      Ubuntu 24.04 + JDK 24 (Temurin) + Arrow 24 + aws-sdk-cpp,
#                installed with LakeVilla's setup_ubuntu.sh
#   lv-build     compiles liblakevilla.so (JNI library) from LV/src
#   lv-lib       export target: docker build --target lv-lib --output <dir>
#   trino        Trino server (trino-server-core + tpch + lakevilla plugins)

ARG JDK_IMAGE=eclipse-temurin:24-jdk

FROM ${JDK_IMAGE} AS lv-deps
ARG ARROW_VERSION=24.0.0-1
ARG AWS_SDK_VERSION=1.11.880
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update && \
    apt-get -qq install -y --no-install-recommends lsb-release wget ca-certificates git && \
    rm -rf /var/lib/apt/lists/*
COPY --from=lakevilla setup_ubuntu.sh /tmp/setup_ubuntu.sh
RUN ARROW_VERSION="$ARROW_VERSION" AWS_SDK_VERSION="$AWS_SDK_VERSION" \
    AWS_PREFIX=/opt/aws bash /tmp/setup_ubuntu.sh && \
    rm -rf /var/lib/apt/lists/* /tmp/setup_ubuntu.sh
ENV LD_LIBRARY_PATH=/opt/aws/lib

FROM lv-deps AS lv-build
# commit of the LV/src/rapidjson submodule of the LakeVilla repository
ARG RAPIDJSON_COMMIT=24b5e7a8b27f42fa16b96fc70aade9106cf7102f
COPY --from=lakevilla src /lv/src
# the rapidjson submodule is fetched if it is not checked out
RUN if [ ! -f /lv/src/rapidjson/include/rapidjson/document.h ]; then \
      rm -rf /lv/src/rapidjson && \
      git clone --quiet https://github.com/Tencent/rapidjson.git /lv/src/rapidjson && \
      git -C /lv/src/rapidjson checkout --quiet "$RAPIDJSON_COMMIT"; \
    fi
RUN cmake -S /lv/src -B /lv/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DAWSSDK_ROOT_DIR=/opt/aws -DLV_BUILD_BENCHMARKS=OFF -DLV_BUILD_COMPAT=OFF \
      -DLV_BUILD_JNI=ON && \
    cmake --build /lv/build --target lakevilla && \
    nm -D --defined-only /lv/build/liblakevilla.so | grep -q Java_io_trino_plugin_lakevilla_LakeVillaNative_ && \
    mkdir -p /out && cp /lv/build/liblakevilla.so /out/

# export target: docker build --target lv-lib --output type=local,dest=<dir>
FROM scratch AS lv-lib
COPY --from=lv-build /out/ /

FROM lv-deps AS trino
ARG TRINO_VERSION=477-SNAPSHOT
# (uid/gid 1000 belong to the user ubuntu of the base image)
RUN groupadd --system trino && \
    useradd --system --gid trino --create-home trino && \
    mkdir -p /usr/lib/trino /data/trino /etc/trino && \
    chown -R trino:trino /usr/lib/trino /data/trino /etc/trino
# trino-server-core with the tpch and lakevilla plugins
ADD --chown=trino:trino dist/trino-server-core-${TRINO_VERSION}.tar.gz /tmp/
RUN mv /tmp/trino-server-core-${TRINO_VERSION}/* /usr/lib/trino/ && rm -rf /tmp/trino-server-core-${TRINO_VERSION}
COPY --chown=trino:trino dist/plugin/ /usr/lib/trino/plugin/
COPY --chown=trino:trino --from=lv-build /out/liblakevilla.so /usr/lib/trino/plugin/lakevilla/
COPY --chown=trino:trino dist/trino-cli /usr/bin/trino
COPY --chown=trino:trino etc/ /etc/trino/
RUN chmod +x /usr/bin/trino
EXPOSE 8080
USER trino:trino
CMD ["/usr/lib/trino/bin/launcher", "run", "--etc-dir", "/etc/trino"]
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s \
  CMD curl -fsS http://localhost:8080/v1/info | grep -q '"starting":false'
