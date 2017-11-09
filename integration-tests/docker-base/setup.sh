#!/bin/bash

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
wget -q -O - http://www.us.apache.org/dist/zookeeper/zookeeper-3.4.10/zookeeper-3.4.10.tar.gz | tar -xzf - -C /usr/local \
  && cp /usr/local/zookeeper-3.4.10/conf/zoo_sample.cfg /usr/local/zookeeper-3.4.10/conf/zoo.cfg \
  && ln -s /usr/local/zookeeper-3.4.10 /usr/local/zookeeper

# Kafka
# Match the version to the Kafka client used by KafkaSupervisor
wget -q -O - http://www.us.apache.org/dist/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz | tar -xzf - -C /usr/local \
 && ln -s /usr/local/kafka_2.11-0.10.2.0 /usr/local/kafka

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
