#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

ARG JDK_VERSION=17

# The platform is explicitly specified as x64 to build the Druid distribution.
# This is because it's not able to build the distribution on arm64 due to dependency problem of web-console. See: https://github.com/apache/druid/issues/13012
# Since only java jars are shipped in the final image, it's OK to build the distribution on x64.
# Once the web-console dependency problem is resolved, we can remove the --platform directive.
FROM --platform=linux/amd64 maven:3.8.6-jdk-11-slim as builder

# Rebuild from source in this stage
# This can be unset if the tarball was already built outside of Docker
ARG BUILD_FROM_SOURCE="true"

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get -qq update \
    && apt-get -qq -y install --no-install-recommends python3 python3-yaml

COPY . /src
WORKDIR /src
RUN --mount=type=cache,target=/root/.m2 if [ "$BUILD_FROM_SOURCE" = "true" ]; then \
      mvn -B -ff -q \
      install \
      -Pdist,bundle-contrib-exts \
      -Pskip-static-checks,skip-tests \
      -Dmaven.javadoc.skip=true -T1C \
      ; fi

RUN --mount=type=cache,target=/root/.m2 VERSION=$(mvn -B -q org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate \
      -Dexpression=project.version -DforceStdout=true \
    ) \
 && tar -zxf ./distribution/target/apache-druid-${VERSION}-bin.tar.gz -C /opt \
 && mv /opt/apache-druid-${VERSION} /opt/druid

FROM alpine:3 as bash-static
ARG TARGETARCH
#
# Download bash-static binary to execute scripts that require bash.
# Although bash-static supports multiple platforms, but there's no need for us to support all those platform, amd64 and arm64 are enough.
#
ARG BASH_URL_BASE="https://github.com/robxu9/bash-static/releases/download/5.1.016-1.2.3"
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      BASH_URL="${BASH_URL_BASE}/bash-linux-aarch64" ; \
    elif [ "$TARGETARCH" = "amd64" ]; then \
      BASH_URL="${BASH_URL_BASE}/bash-linux-x86_64" ; \
    else \
      echo "Unsupported architecture ($TARGETARCH)" && exit 1; \
    fi; \
    echo "Downloading bash-static from ${BASH_URL}" \
    && wget ${BASH_URL} -O /bin/bash

FROM busybox:1.35.0-glibc as busybox

FROM gcr.io/distroless/java$JDK_VERSION-debian12
LABEL maintainer="Apache Druid Developers <dev@druid.apache.org>"

COPY --from=busybox /bin/busybox /busybox/busybox
RUN ["/busybox/busybox", "--install", "/bin"]


RUN addgroup -S -g 1000 druid \
 && adduser -S -u 1000 -D -H -h /opt/druid -s /bin/sh -g '' -G druid druid


COPY --from=bash-static /bin/bash /bin/bash
RUN chmod 755 /bin/bash

COPY distribution/docker/druid.sh /druid.sh
COPY distribution/docker/peon.sh /peon.sh
COPY distribution/docker/deduplicate_jars.sh /deduplicate_jars.sh

# create necessary directories which could be mounted as volume
# copy and de-duplicate jars from builder in same layer to reduce image size
#   /opt/druid/var is used to keep individual files(e.g. log) of each Druid service
#   /opt/shared is used to keep segments and task logs shared among Druid services
RUN --mount=type=bind,from=builder,source=/opt,target=/builder/opt \
 mkdir -p /opt/druid/var /opt/shared \
 && cp -r /builder/opt/druid /opt/ \
 && /deduplicate_jars.sh /opt/druid \
 && chown -R druid:druid /opt/druid \
 && chmod 775 /opt/druid/var /opt/shared

USER druid
VOLUME /opt/druid/var
WORKDIR /opt/druid

ENTRYPOINT ["/druid.sh"]
