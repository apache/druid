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

FROM alpine as extractor

ARG VERSION=

WORKDIR /src
COPY ./target/apache-druid-${VERSION}-bin.tar.gz .

RUN tar -zxf /src/apache-druid-${VERSION}-bin.tar.gz -C /opt \
 && ln -s /opt/apache-druid-${VERSION} /opt/druid

FROM amd64/busybox:1.30.0-glibc as busybox

FROM gcr.io/distroless/java:8
LABEL maintainer="Apache Druid Developers <dev@druid.apache.org>"

COPY --from=busybox /bin/busybox /busybox/busybox
RUN ["/busybox/busybox", "--install", "/bin"]

COPY --from=extractor /opt /opt
COPY ./docker/druid.sh /druid.sh

RUN addgroup -S -g 1000 druid \
 && adduser -S -u 1000 -D -H -h /opt/druid -s /bin/sh -g '' -G druid druid \
 && mkdir -p /opt/druid/var \
 && chown -R druid:druid /opt \
 && chmod 775 /opt/druid/var

USER druid
VOLUME /opt/druid/var
WORKDIR /opt/druid

ENTRYPOINT ["/druid.sh"]
