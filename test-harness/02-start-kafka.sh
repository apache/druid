#!/bin/bash
(
source env-cluster.sh
sed s/zk\.connect.*/zk.connect=$ZK_CONNECT_STRING/ kafka/config/server.properties > /tmp/kafka-server.properties
mkdir -p logs
cd kafka
bin/kafka-server-start.sh /tmp/kafka-server.properties >> ../logs/kafka.log 2>&1 &
)
