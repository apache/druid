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

FROM maven:3-jdk-8 as builder

RUN apt-get update && apt-get install --no-install-recommends -y python3-pip python3-setuptools python3-wheel\
  && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir pyyaml

COPY . /src
WORKDIR /src
RUN mvn dependency:go-offline install -ff -q -B -DskipTests -Dforbiddenapis.skip=true -Pdist -Pbundle-contrib-exts

RUN \
 VER=$(mvn -B org.apache.maven.plugins:maven-help-plugin:3.1.1:evaluate -Dexpression=project.version -q -DforceStdout=true -f pom.xml 2>/dev/null) \
 && tar -zxf ./distribution/target/apache-druid-${VER}-bin.tar.gz -C /opt \
 && ln -s /opt/apache-druid-${VER} /opt/druid

RUN addgroup --gid 1000 druid \
 && adduser --home /opt/druid --shell /bin/sh --no-create-home --uid 1000 --gecos '' --gid 1000 --disabled-password druid \
 && mkdir -p /opt/druid/var \
 && chown -R druid:druid /opt/druid \
 && chmod 775 /opt/druid/var

FROM amd64/busybox:1.30.0-glibc as busybox
FROM gcr.io/distroless/java:8
LABEL maintainer="Apache Druid Developers <dev@druid.apache.org>"

COPY --from=busybox /bin/busybox /busybox/busybox
RUN ["/busybox/busybox", "--install", "/bin"]
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder --chown=druid /opt /opt
COPY distribution/docker/druid.sh /druid.sh
RUN chown -R druid:druid /opt/druid
USER druid
VOLUME /opt/druid/var
WORKDIR /opt/druid

ENTRYPOINT ["/druid.sh"]
