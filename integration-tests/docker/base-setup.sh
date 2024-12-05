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
APACHE_ARCHIVE_MIRROR_HOST=${APACHE_ARCHIVE_MIRROR_HOST:-https://archive.apache.org}

apt-get update

# wget
apt-get install -y wget

# MySQL (Metadata store)
apt-get install -y default-mysql-server

# Supervisor
apt-get install -y supervisor

# Retry function to make setup process more robust
retry() {
  local retries="$1"
  shift
  local count=0
  local success=false

  while [ $count -lt "$retries" ]; do
    echo "Attempting command... (Attempt $((count + 1)) of $retries)"

    if "$@"; then
      success=true
      break
    else
      count=$((count + 1))
      sleep 2  # Optional: wait before retrying
    fi
  done

  if [ "$success" = false ]; then
    echo "Command failed after $retries attempts."
    exit 1
  fi
}

# Zookeeper
install_zk() {
  retry 3 wget -q -O /tmp/$ZK_TAR.tar.gz "$APACHE_ARCHIVE_MIRROR_HOST/dist/zookeeper/zookeeper-$ZK_VERSION/$ZK_TAR.tar.gz"
  tar -xzf /tmp/$ZK_TAR.tar.gz -C /usr/local
  cp /usr/local/$ZK_TAR/conf/zoo_sample.cfg /usr/local/$ZK_TAR/conf/zoo.cfg
  rm /tmp/$ZK_TAR.tar.gz
}

ZK_TAR=apache-zookeeper-$ZK_VERSION-bin
install_zk
ln -s /usr/local/$ZK_TAR /usr/local/zookeeper

# Kafka
# KAFKA_VERSION is defined by docker build arguments
wget -q -O /tmp/kafka_2.13-$KAFKA_VERSION.tgz "$APACHE_ARCHIVE_MIRROR_HOST/dist/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz"
tar -xzf /tmp/kafka_2.13-$KAFKA_VERSION.tgz -C /usr/local
ln -s /usr/local/kafka_2.13-$KAFKA_VERSION /usr/local/kafka
rm /tmp/kafka_2.13-$KAFKA_VERSION.tgz

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
