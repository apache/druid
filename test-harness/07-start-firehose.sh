#!/bin/bash
mkdir -p logs
source env-cluster.sh

(
cd firehose 
bin/start-firehose.sh -rate $FIREHOSE_RATE_PER_SEC -zk "$ZK_CONNECT_STRING" -out kafka -topic $KAFKA_TOPIC  >> ../logs/firehose.log 2>&1 &
)
