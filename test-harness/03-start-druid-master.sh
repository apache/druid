#!/bin/bash
(
source env-cluster.sh
mkdir -p logs
# bug in druid...it doesn't create this node
echo -e "create /druid foo\ncreate /druid/announcements foo" | kafka/bin/zookeeper-shell.sh "$ZK_CONNECT_STRING"
echo "Ignore the 'already exists' errors"

cd druid 
bin/start-master.sh >> ../logs/druid-master.log 2>&1 &
)
