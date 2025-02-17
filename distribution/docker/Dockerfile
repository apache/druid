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

# By default we build the entire distribution from source in docker
# This can be unset if the tarball was already built outside of Docker
ARG BUILD_FROM_SOURCE="true"

#===============================================================
# web-console building stage
#===============================================================
# The platform is explicitly specified as x64 to build the Druid distribution.
# This is because it's not able to build the distribution on arm64 due to dependency problem of web-console. See: https://github.com/apache/druid/issues/13012
# Since only java jars are shipped in the final image, it's OK to build the distribution on x64.
# Once the web-console dependency problem is resolved, we can remove the --platform directive.
FROM --platform=linux/amd64 node:20.9.0-slim as web-console-builder

RUN npm install -g npm@10.9.0

# Only copy necessary files to web-console to build to try the best to use docker cache
COPY ./web-console /src/web-console
COPY ./docs /src/docs
WORKDIR /src/web-console

# Re-declare the ARG to make it available in this stage
ARG BUILD_FROM_SOURCE

# Suppress a building error saying error:0308010C:digital envelope routines::unsupported
ENV NODE_OPTIONS=--openssl-legacy-provider

# The directory of output, during the package stage, files will be copied from this directory
RUN mkdir /output

# The ls command below is just for debug purpose helping us know which files are updated that docker cache are invalidated
RUN if [ "$BUILD_FROM_SOURCE" = "true" ]; then \
  ls -l && \
  npm ci \
; fi
RUN if [ "$BUILD_FROM_SOURCE" = "true" ]; then \
   ./script/build \
   && ./script/cp-to ./output \
; fi

#===============================================================
# Declare a builder stage to build the Druid distribution
#===============================================================
FROM maven:3.9.9-eclipse-temurin-${JDK_VERSION} as builder
RUN apt-get -qq update \
    && apt-get -qq -y install --no-install-recommends python3 python3-yaml

COPY . /src
WORKDIR /src

# Copy the web-console resources from the web-console-builder to include it in this stage to package
COPY --from=web-console-builder /output /src/web-console/target/resources

# Re-declare the ARG to make it available in this stage
ARG BUILD_FROM_SOURCE

# Note: use web.console.skip to exclude building of web-console but include the package of web-console
RUN --mount=type=cache,target=/root/.m2 if [ "$BUILD_FROM_SOURCE" = "true" ]; then \
      rm -fr distribution/target/ && \
      mvn -B -ff -ntp \
      install \
      -Pdist,bundle-contrib-exts \
      -Pskip-static-checks,skip-tests \
      -Dweb.console.skip=true \
      -pl '!org.apache.druid:druid-benchmarks, !org.apache.druid:druid-integration-tests, !org.apache.druid.integration-tests:druid-it-tools, !org.apache.druid.integration-tests:druid-it-image, !org.apache.druid.integration-tests:druid-it-cases' \
      -T1C \
; fi


# RUN ls ./distribution/target
RUN DISTRIBUTION=$(ls ./distribution/target/apache-druid-*-bin.tar.gz) \
 && tar -zxf ${DISTRIBUTION} -C /opt \
 && mv /opt/apache-druid-* /opt/druid

#===============================================================
# Final stage
#===============================================================
# Above bulding staging uses images based on eclipse-temurin,
# however here we directly use alpine because eclipse-temurin-alpine for JRE17 does not ship a image with ARM64 architecture.
FROM alpine:latest
LABEL maintainer="Apache Druid Developers <dev@druid.apache.org>"

# Re-daclare the ARG to make it available in this stage
ARG JDK_VERSION

# Install necessary packages to easier debugging
RUN apk add --no-cache openjdk${JDK_VERSION}-jre bash curl net-tools strace lsof \
 && addgroup -S -g 1000 druid \
 && adduser -S -u 1000 -D -H -h /opt/druid -s /bin/sh -g '' -G druid druid

ENV JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk

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
 && chown -R druid:druid /opt/druid /opt/shared \
 && chmod 775 /opt/druid/var /opt/shared

USER druid
VOLUME /opt/druid/var
WORKDIR /opt/druid

ENTRYPOINT ["/druid.sh"]
