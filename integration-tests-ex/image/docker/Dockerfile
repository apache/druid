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
#-------------------------------------------------------------------------

# Builds the Druid image for testing. Does not include dependent
# tools such as MySQL, Zookeeper or Kafka: those reside in their own images.
#
# The script assumes that Maven as placed a Druid distribution and the
# required extra jars in the same location as this file: in the target/docker
# directory. You must run Maven once to populate these files after each
# build. After that first image build, you can use rebuild.sh to do follow-on
# image builds.

# This Dockerfile prefers to use the COPY command over ADD.
# See: https://phoenixnap.com/kb/docker-add-vs-copy

ARG JDK_VERSION=8-slim-buster

# The FROM image provides Java on top of Debian, and
# thus provides bash, apt-get, etc.
# See https://hub.docker.com/_/openjdk

FROM openjdk:$JDK_VERSION

ARG DRUID_VERSION
ENV DRUID_VERSION=$DRUID_VERSION
ARG CONFLUENT_VERSION
ENV CONFLUENT_VERSION=$CONFLUENT_VERSION
ARG MYSQL_VERSION
ENV MYSQL_VERSION=$MYSQL_VERSION
ARG MARIADB_VERSION
ENV MARIADB_VERSION=$MARIADB_VERSION
ARG MYSQL_DRIVER_CLASSNAME=com.mysql.jdbc.Driver
ENV MYSQL_DRIVER_CLASSNAME=$MYSQL_DRIVER_CLASSNAME

ENV DRUID_HOME=/usr/local/druid

# Populate build artifacts

COPY apache-druid-${DRUID_VERSION}-bin.tar.gz /usr/local/
COPY druid-it-tools-${DRUID_VERSION}.jar /tmp/druid/extensions/druid-it-tools/
COPY kafka-protobuf-provider-${CONFLUENT_VERSION}.jar /tmp/druid/lib/
COPY mysql-connector-java-${MYSQL_VERSION}.jar /tmp/druid/lib/
COPY mariadb-java-client-${MARIADB_VERSION}.jar /tmp/druid/lib/
COPY test-setup.sh /
COPY druid.sh /
COPY launch.sh /

# Do the setup tasks. The tasks are done within a script, rather than
# here, so they are easier to describe and debug. Turn on the "-x" flag
# within the script to trace the steps if needed for debugging.

RUN bash /test-setup.sh

# Start in the Druid directory.

USER druid:druid
WORKDIR /
ENTRYPOINT [ "bash", "/launch.sh" ]
