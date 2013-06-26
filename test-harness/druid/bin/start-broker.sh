#!/bin/bash
ENV_CLUSTER="`dirname $0`/../../env-cluster.sh"
source $ENV_CLUSTER

NODE_TYPE=broker
MAIN_CLASS=com.metamx.druid.http.BrokerMain
DRUID_HOST="${DRUID_BROKER_HOST}:${DRUID_BROKER_PORT}"
DRUID_PORT=$DRUID_BROKER_PORT

mkdir -p /tmp/druid-$NODE_TYPE
CONFIG="`dirname $0`/../config"
RT_CONFIG="/tmp/druid-${NODE_TYPE}/runtime.properties"

cat $CONFIG/base.properties $CONFIG/${NODE_TYPE}.properties | 
  sed s/druid\.zk\.service\.host\=.*/druid.zk.service.host=$ZK_CONNECT_STRING/g | 
  sed s/com\.metamx\.aws\.accessKey\=.*/com.metamx.aws.accessKey=$AWS_KEY/g | 
  sed s/com\.metamx\.aws\.secretKey\=.*/com.metamx.aws.secretKey=$AWS_SECRET/g | 
  sed s/druid\.pusher\.s3\.bucket\=.*/druid.pusher.s3.bucket=$S3_BUCKET/g  | 
  sed s/druid\.pusher\.s3\.baseKey\=.*/druid.pusher.s3.baseKey=$S3_BASENAME/g | 
  sed s/druid\.host\=.*/druid.host=$DRUID_HOST/g | 
  sed s/druid\.port\=.*/druid.port=$DRUID_PORT/g > $RT_CONFIG
java -cp /tmp/druid-${NODE_TYPE}:`ls *.jar | tr '\n' ':'` $MAIN_CLASS
