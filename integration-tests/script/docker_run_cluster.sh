#!/usr/bin/env bash

# Create docker network
{
  docker network create --subnet=172.172.172.0/24 druid-it-net
}

# setup all enviornment variables to be pass to the containers
COMMON_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/common -e DRUID_INTEGRATION_TEST_GROUP"
BROKER_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/broker"
COORDINATOR_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/coordinator"
HISTORICAL_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/historical"
MIDDLEMANAGER_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/middlemanager"
OVERLORD_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/overlord"
ROUTER_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/router"
ROUTER_CUSTOM_CHECK_TLS_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/router-custom-check-tls"
ROUTER_NO_CLIENT_AUTH_TLS_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/router-no-client-auth-tls"
ROUTER_PERMISSIVE_TLS_ENV="--env-file=$ENVIRONMENT_CONFIGS_DIR/router-permissive-tls"

OVERRIDE_ENV=""
if [ -z "$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH" ]
then
    echo "\$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH is not set. No override config file provided"
    if [ "$DRUID_INTEGRATION_TEST_GROUP" = "s3-deep-storage" ] || \
    [ "$DRUID_INTEGRATION_TEST_GROUP" = "gcs-deep-storage" ] || \
    [ "$DRUID_INTEGRATION_TEST_GROUP" = "azure-deep-storage" ]; then
      echo "Test group $DRUID_INTEGRATION_TEST_GROUP requires override config file. Stopping test..."
      exit 1
    fi
else
    echo "\$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH is set with value ${DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH}"
    OVERRIDE_ENV="--env-file=$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH"
fi

# Start docker containers for all Druid processes and dependencies
{
  # Start Hadoop docker if needed
  if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
  then
    # Start Hadoop docker container
    docker run -d --privileged --net druid-it-net --ip 172.172.172.13 -h druid-it-hadoop --name druid-it-hadoop -p 2049:2049 -p 2122:2122 -p 8020:8020 -p 8021:8021 -p 8030:8030 -p 8031:8031 -p 8032:8032 -p 8033:8033 -p 8040:8040 -p 8042:8042 -p 8088:8088 -p 8443:8443 -p 9000:9000 -p 10020:10020 -p 19888:19888 -p 34455:34455 -p 49707:49707 -p 50010:50010 -p 50020:50020 -p 50030:50030 -p 50060:50060 -p 50070:50070 -p 50075:50075 -p 50090:50090 -p 51111:51111 -v $RESOURCEDIR:/resources -v $SHARED_DIR:/shared druid-it/hadoop:2.8.5 sh -c "/etc/bootstrap.sh && tail -f /dev/null"

    sh ./copy_hadoop_resources.sh
  fi

  # Start zookeeper and kafka
  docker run -d --privileged --net druid-it-net --ip 172.172.172.2 ${COMMON_ENV} --name druid-zookeeper-kafka -p 2181:2181 -p 9092:9092 -p 9093:9093 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/zookeeper.conf:$SUPERVISORDIR/zookeeper.conf -v $SERVICE_SUPERVISORDS_DIR/kafka.conf:$SUPERVISORDIR/kafka.conf druid/cluster

  # Start MYSQL
  docker run -d --privileged --net druid-it-net --ip 172.172.172.3 ${COMMON_ENV} --name druid-metadata-storage -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/metadata-storage.conf:$SUPERVISORDIR/metadata-storage.conf druid/cluster

  # Start Overlord
  docker run -d --privileged --net druid-it-net --ip 172.172.172.4 ${COMMON_ENV} ${OVERLORD_ENV} ${OVERRIDE_ENV} --name druid-overlord -p 8090:8090 -p 8290:8290 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-metadata-storage:druid-metadata-storage --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

  # Start Coordinator
  docker run -d --privileged --net druid-it-net --ip 172.172.172.5 ${COMMON_ENV} ${COORDINATOR_ENV} ${OVERRIDE_ENV} --name druid-coordinator -p 8081:8081 -p 8281:8281 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-overlord:druid-overlord --link druid-metadata-storage:druid-metadata-storage --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

  # Start Historical
  docker run -d --privileged --net druid-it-net --ip 172.172.172.6 ${COMMON_ENV} ${HISTORICAL_ENV} ${OVERRIDE_ENV} --name druid-historical -p 8083:8083 -p 8283:8283 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

  # Start Middlemanger
  docker run -d --privileged --net druid-it-net --ip 172.172.172.7 ${COMMON_ENV} ${MIDDLEMANAGER_ENV} ${OVERRIDE_ENV} --name druid-middlemanager -p 8091:8091 -p 8291:8291 -p 8100:8100 -p 8101:8101 -p 8102:8102 -p 8103:8103 -p 8104:8104 -p 8105:8105 -p 8300:8300 -p 8301:8301 -p 8302:8302 -p 8303:8303 -p 8304:8304 -p 8305:8305 -v $RESOURCEDIR:/resources -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-overlord:druid-overlord druid/cluster

  # Start Broker
  docker run -d --privileged --net druid-it-net --ip 172.172.172.8 ${COMMON_ENV} ${BROKER_ENV} ${OVERRIDE_ENV} --name druid-broker -p 8082:8082 -p 8282:8282 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-middlemanager:druid-middlemanager --link druid-historical:druid-historical druid/cluster

  # Start Router
  docker run -d --privileged --net druid-it-net --ip 172.172.172.9 ${COMMON_ENV} ${ROUTER_ENV} ${OVERRIDE_ENV} --name druid-router -p 8888:8888 -p 9088:9088 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

  # Start Router with permissive TLS settings (client auth enabled, no hostname verification, no revocation check)
  docker run -d --privileged --net druid-it-net --ip 172.172.172.10 ${COMMON_ENV} ${ROUTER_PERMISSIVE_TLS_ENV} ${OVERRIDE_ENV} --name druid-router-permissive-tls -p 8889:8889 -p 9089:9089 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

  # Start Router with TLS but no client auth
  docker run -d --privileged --net druid-it-net --ip 172.172.172.11 ${COMMON_ENV} ${ROUTER_NO_CLIENT_AUTH_TLS_ENV} ${OVERRIDE_ENV} --name druid-router-no-client-auth-tls -p 8890:8890 -p 9090:9090 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

  # Start Router with custom TLS cert checkers
  docker run -d --privileged --net druid-it-net --ip 172.172.172.12 ${COMMON_ENV} ${ROUTER_CUSTOM_CHECK_TLS_ENV} ${OVERRIDE_ENV} --hostname druid-router-custom-check-tls --name druid-router-custom-check-tls -p 8891:8891 -p 9091:9091 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster
}