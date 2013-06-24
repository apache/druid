#!/bin/bash
(
source env-cluster.sh
sed s/clientPort.*/clientPort=$ZOOKEEPER_PORT/ kafka/config/zookeeper.properties > /tmp/zookeeper.properties
mkdir -p logs
cd kafka
bin/zookeeper-server-start.sh /tmp/zookeeper.properties >> ../logs/zookeeper.log 2>&1 &
)

