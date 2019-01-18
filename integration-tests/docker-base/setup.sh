#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -u

export DEBIAN_FRONTEND=noninteractive

apt-get update

# wget
apt-get install -y wget

# Java
apt-get install -y openjdk-8-jdk

# MySQL (Metadata store)
apt-get install -y mysql-server

# Supervisor
apt-get install -y supervisor

# Zookeeper
wget -q -O - http://www.us.apache.org/dist/zookeeper/zookeeper-3.4.11/zookeeper-3.4.11.tar.gz | tar -xzf - -C /usr/local \
  && cp /usr/local/zookeeper-3.4.11/conf/zoo_sample.cfg /usr/local/zookeeper-3.4.11/conf/zoo.cfg \
  && ln -s /usr/local/zookeeper-3.4.11 /usr/local/zookeeper

# Kafka
# Match the version to the Kafka client used by KafkaSupervisor
wget -q -O - http://www.us.apache.org/dist/kafka/0.10.2.2/kafka_2.12-0.10.2.2.tgz | tar -xzf - -C /usr/local \
 && ln -s /usr/local/kafka_2.12-0.10.2.2 /usr/local/kafka

# Druid system user
adduser --system --group --no-create-home druid \
  && mkdir -p /var/lib/druid \
  && chown druid:druid /var/lib/druid

# clean up time
apt-get clean \
  && rm -rf /tmp/* \
            /var/tmp/* \
            /var/lib/apt/lists \
            /root/.m2
