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

ARG JDK_VERSION=8
FROM maven:3.8.1-jdk-11-slim as builder
# Rebuild from source in this stage
# This can be unset if the tarball was already built outside of Docker
ARG BUILD_FROM_SOURCE="true"

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get -qq update \
    && apt-get -qq -y install --no-install-recommends python3 python3-yaml

COPY . /src
WORKDIR /src
RUN if [ "$BUILD_FROM_SOURCE" = "true" ]; then \
      mvn -B -ff -q dependency:go-offline \
      install \
      -Pdist,bundle-contrib-exts \
      -Pskip-static-checks,skip-tests \
      -Dmaven.javadoc.skip=true \
      ; fi

RUN VERSION=$(mvn -B -q org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate \
      -Dexpression=project.version -DforceStdout=true \
    ) \
 && tar -zxf ./distribution/target/apache-druid-${VERSION}-bin.tar.gz -C /opt \
 && mv /opt/apache-druid-${VERSION} /opt/druid

FROM amd64/busybox:1.30.0-glibc as busybox

FROM gcr.io/distroless/java:$JDK_VERSION
LABEL maintainer="Apache Druid Developers <dev@druid.apache.org>"

COPY --from=busybox /bin/busybox /busybox/busybox
RUN ["/busybox/busybox", "--install", "/bin"]

RUN addgroup -S -g 1000 druid \
 && adduser -S -u 1000 -D -H -h /opt/druid -s /bin/sh -g '' -G druid druid

COPY --chown=druid:druid --from=builder /opt /opt
COPY distribution/docker/druid.sh /druid.sh

# create necessary directories which could be mounted as volume
#   /opt/druid/var is used to keep individual files(e.g. log) of each Druid service
#   /opt/shared is used to keep segments and task logs shared among Druid services
RUN mkdir /opt/druid/var /opt/shared \
 && chown druid:druid /opt/druid/var /opt/shared \
 && chmod 775 /opt/druid/var /opt/shared

USER druid
VOLUME /opt/druid/var
WORKDIR /opt/druid

ENTRYPOINT ["/druid.sh"]
