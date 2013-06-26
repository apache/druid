#!/bin/bash
ENV_CLUSTER="`dirname $0`/../../env-cluster.sh"
source $ENV_CLUSTER

NODE_TYPE=realtime
MAIN_CLASS=com.metamx.druid.realtime.RealtimeMain
DRUID_HOST="${DRUID_REALTIME_HOST}:${DRUID_REALTIME_PORT}"
DRUID_PORT=$DRUID_REALTIME_PORT

mkdir -p /tmp/druid-$NODE_TYPE
CONFIG="`dirname $0`/../config"
RT_CONFIG="/tmp/druid-${NODE_TYPE}/runtime.properties"
SPEC="`dirname $0`/../appevents_realtime.spec"
RT_SPEC="`dirname $0`/../appevents_realtime_generated.spec"
cat $CONFIG/base.properties $CONFIG/${NODE_TYPE}.properties |
  sed s/druid\.realtime\.specFile\=.*/druid.realtime.specFile=appevents_realtime_generated.spec/g |
  sed s/druid\.zk\.service\.host\=.*/druid.zk.service.host=$ZK_CONNECT_STRING/g |
  sed s/com\.metamx\.aws\.accessKey\=.*/com.metamx.aws.accessKey=$AWS_KEY/g |
  sed s/com\.metamx\.aws\.secretKey\=.*/com.metamx.aws.secretKey=$AWS_SECRET/g |
  sed s/druid\.pusher\.s3\.bucket\=.*/druid.pusher.s3.bucket=$S3_BUCKET/g  |
  sed s/druid\.pusher\.s3\.baseKey\=.*/druid.pusher.s3.baseKey=$S3_BASENAME/g |
  sed s/druid\.host\=.*/druid.host=$DRUID_HOST/g |
  sed s/druid\.port\=.*/druid.port=$DRUID_PORT/g > $RT_CONFIG

cat $SPEC |
  sed s/\"zk\.connect\".*,/\"zk.connect\":\"$ZK_CONNECT_STRING\",/g |
  sed s/\"feed\".*,/\"feed\":\"$KAFKA_TOPIC\",/g  > $RT_SPEC

java -cp /tmp/druid-${NODE_TYPE}:`ls *.jar | tr '\n' ':'` $MAIN_CLASS
