#!/usr/bin/env bash
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

# Cleanup old images/containers
{
  for node in druid-historical druid-coordinator druid-overlord druid-router druid-router-permissive-tls druid-router-no-client-auth-tls druid-router-custom-check-tls druid-broker druid-middlemanager druid-zookeeper-kafka druid-metadata-storage druid-it-hadoop;
  do
  docker stop $node
  docker rm $node
  done

  docker network rm druid-it-net
}

# Druid environment and jars setup
{
  # environment variables
  DIR=$(cd $(dirname $0) && pwd)
  HADOOP_DOCKER_DIR=$DIR/../examples/quickstart/tutorial/hadoop/docker
  DOCKERDIR=$DIR/docker
  SERVICE_SUPERVISORDS_DIR=$DOCKERDIR/service-supervisords
  ENVIRONMENT_CONFIGS_DIR=$DOCKERDIR/environment-configs
  SHARED_DIR=${HOME}/shared
  SUPERVISORDIR=/usr/lib/druid/conf
  RESOURCEDIR=$DIR/src/test/resources

  # so docker IP addr will be known during docker build
  echo ${DOCKER_IP:=127.0.0.1} > $DOCKERDIR/docker_ip

  # setup client keystore
  ./docker/tls/generate-client-certs-and-keystores.sh
  rm -rf docker/client_tls
  cp -r client_tls docker/client_tls

  # Make directories if they dont exist
  mkdir -p $SHARED_DIR/hadoop_xml
  mkdir -p $SHARED_DIR/hadoop-dependencies
  mkdir -p $SHARED_DIR/logs
  mkdir -p $SHARED_DIR/tasklogs
  mkdir -p $SHARED_DIR/docker/extensions
  mkdir -p $SHARED_DIR/docker/credentials

  # install druid jars
  rm -rf $SHARED_DIR/docker
  cp -R docker $SHARED_DIR/docker
  mvn -B dependency:copy-dependencies -DoutputDirectory=$SHARED_DIR/docker/lib

  # move extensions into a seperate extension folder
  # For druid-s3-extensions
  mkdir -p $SHARED_DIR/docker/extensions/druid-s3-extensions
  mv $SHARED_DIR/docker/lib/druid-s3-extensions-* $SHARED_DIR/docker/extensions/druid-s3-extensions
  # For druid-azure-extensions
  mkdir -p $SHARED_DIR/docker/extensions/druid-azure-extensions
  mv $SHARED_DIR/docker/lib/druid-azure-extensions-* $SHARED_DIR/docker/extensions/druid-azure-extensions
  # For druid-google-extensions
  mkdir -p $SHARED_DIR/docker/extensions/druid-google-extensions
  mv $SHARED_DIR/docker/lib/druid-google-extensions-* $SHARED_DIR/docker/extensions/druid-google-extensions
  # For druid-hdfs-storage
  mkdir -p $SHARED_DIR/docker/extensions/druid-hdfs-storage
  mv $SHARED_DIR/docker/lib/druid-hdfs-storage-* $SHARED_DIR/docker/extensions/druid-hdfs-storage
  # For druid-kinesis-indexing-service
  mkdir -p $SHARED_DIR/docker/extensions/druid-kinesis-indexing-service
  mv $SHARED_DIR/docker/lib/druid-kinesis-indexing-service-* $SHARED_DIR/docker/extensions/druid-kinesis-indexing-service

  # Pull Hadoop dependency if needed
  if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
  then
    java -cp "$SHARED_DIR/docker/lib/*" -Ddruid.extensions.hadoopDependenciesDir="$SHARED_DIR/hadoop-dependencies" org.apache.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client:2.8.5 -h org.apache.hadoop:hadoop-aws:2.8.5
  fi

  # install logging config
  cp src/main/resources/log4j2.xml $SHARED_DIR/docker/lib/log4j2.xml

  # copy the integration test jar, it provides test-only extension implementations
  cp target/druid-integration-tests*.jar $SHARED_DIR/docker/lib

  # one of the integration tests needs the wikiticker sample data
  mkdir -p $SHARED_DIR/wikiticker-it
  cp ../examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz $SHARED_DIR/wikiticker-it/wikiticker-2015-09-12-sampled.json.gz
  cp docker/wiki-simple-lookup.json $SHARED_DIR/wikiticker-it/wiki-simple-lookup.json

  # copy other files if needed
  if [ -n "$DRUID_INTEGRATION_TEST_RESOURCE_FILE_DIR_PATH" ]
  then
    cp -a $DRUID_INTEGRATION_TEST_RESOURCE_FILE_DIR_PATH/. $SHARED_DIR/docker/credentials/
  fi

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
}

# Create docker network
{
  docker network create --subnet=172.172.172.0/24 druid-it-net
}

# Build Druid Cluster Image
if [ -z "$DRUID_INTEGRATION_TEST_JVM_RUNTIME" ]
then
      echo "\$DRUID_INTEGRATION_TEST_JVM_RUNTIME is not set. Running integration test with image running Java 8"
      docker build -t druid/cluster --build-arg DOCKER_IMAGE=imply/druiditbase:openjdk-1.8.0_191-1 $SHARED_DIR/docker
else
      echo "\$DRUID_INTEGRATION_TEST_JVM_RUNTIME is set with value ${DRUID_INTEGRATION_TEST_JVM_RUNTIME}"
      case "${DRUID_INTEGRATION_TEST_JVM_RUNTIME}" in
      8)
        echo "Running integration test with image running Java 8"
        docker build -t druid/cluster --build-arg DOCKER_IMAGE=imply/druiditbase:openjdk-1.8.0_191-1 $SHARED_DIR/docker
        ;;
      11)
        echo "Running integration test with image running Java 11"
        docker build -t druid/cluster --build-arg DOCKER_IMAGE=imply/druiditbase:openjdk-11.0.5-1 $SHARED_DIR/docker
        ;;
      *)
        echo "Invalid JVM Runtime given. Stopping"
        exit 1
        ;;
      esac
fi

# Build Hadoop docker if needed
if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
then
  docker build -t druid-it/hadoop:2.8.5 $HADOOP_DOCKER_DIR
fi


# Start docker containers for all Druid processes and dependencies
{
  # Start Hadoop docker if needed
  if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
  then
    # Start Hadoop docker container
    docker run -d --privileged --net druid-it-net --ip 172.172.172.13 -h druid-it-hadoop --name druid-it-hadoop -p 2049:2049 -p 2122:2122 -p 8020:8020 -p 8021:8021 -p 8030:8030 -p 8031:8031 -p 8032:8032 -p 8033:8033 -p 8040:8040 -p 8042:8042 -p 8088:8088 -p 8443:8443 -p 9000:9000 -p 10020:10020 -p 19888:19888 -p 34455:34455 -p 49707:49707 -p 50010:50010 -p 50020:50020 -p 50030:50030 -p 50060:50060 -p 50070:50070 -p 50075:50075 -p 50090:50090 -p 51111:51111 -v $RESOURCEDIR:/resources -v $SHARED_DIR:/shared druid-it/hadoop:2.8.5 sh -c "/etc/bootstrap.sh && tail -f /dev/null"

    # wait for hadoop namenode to be up
    echo "Waiting for hadoop namenode to be up"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /druid"
    while [ $? -ne 0 ]
    do
       sleep 2
       docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /druid"
    done
    echo "Finished waiting for Hadoop namenode"

    # Setup hadoop druid dirs
    echo "Setting up druid hadoop dirs"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /druid"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /druid/segments"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -mkdir -p /quickstart"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod 777 /druid"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod 777 /druid/segments"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod 777 /quickstart"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod -R 777 /tmp"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -chmod -R 777 /user"
    # Copy data files to Hadoop container
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -put /shared/wikiticker-it/wikiticker-2015-09-12-sampled.json.gz /quickstart/wikiticker-2015-09-12-sampled.json.gz"
    docker exec -t druid-it-hadoop sh -c "./usr/local/hadoop/bin/hdfs dfs -put /resources/data/batch_index /batch_index"
    echo "Finished setting up druid hadoop dirs"

    echo "Copying Hadoop XML files to shared"
    docker exec -t druid-it-hadoop sh -c "cp /usr/local/hadoop/etc/hadoop/*.xml /shared/hadoop_xml"
    echo "Copied Hadoop XML files to shared"
  fi

  # Start zookeeper and kafka
  docker run -d --privileged --net druid-it-net --ip 172.172.172.2 ${COMMON_ENV} --name druid-zookeeper-kafka -p 2181:2181 -p 9092:9092 -p 9093:9093 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/zookeeper.conf:$SUPERVISORDIR/zookeeper.conf -v $SERVICE_SUPERVISORDS_DIR/kafka.conf:$SUPERVISORDIR/kafka.conf druid/cluster

  # Start MYSQL
  docker run -d --privileged --net druid-it-net --ip 172.172.172.3 ${COMMON_ENV} --name druid-metadata-storage -p 3306:3306 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/metadata-storage.conf:$SUPERVISORDIR/metadata-storage.conf druid/cluster

  # Start Overlord
  docker run -d --privileged --net druid-it-net --ip 172.172.172.4 ${COMMON_ENV} ${OVERLORD_ENV} ${OVERRIDE_ENV} --name druid-overlord -p 5009:5009 -p 8090:8090 -p 8290:8290 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-metadata-storage:druid-metadata-storage --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

  # Start Coordinator
  docker run -d --privileged --net druid-it-net --ip 172.172.172.5 ${COMMON_ENV} ${COORDINATOR_ENV} ${OVERRIDE_ENV} --name druid-coordinator -p 5006:5006 -p 8081:8081 -p 8281:8281 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-overlord:druid-overlord --link druid-metadata-storage:druid-metadata-storage --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

  # Start Historical
  docker run -d --privileged --net druid-it-net --ip 172.172.172.6 ${COMMON_ENV} ${HISTORICAL_ENV} ${OVERRIDE_ENV} --name druid-historical -p 5007:5007 -p 8083:8083 -p 8283:8283 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

  # Start Middlemanger
  docker run -d --privileged --net druid-it-net --ip 172.172.172.7 ${COMMON_ENV} ${MIDDLEMANAGER_ENV} ${OVERRIDE_ENV} --name druid-middlemanager -p 5008:5008 -p 8091:8091 -p 8291:8291 -p 8100:8100 -p 8101:8101 -p 8102:8102 -p 8103:8103 -p 8104:8104 -p 8105:8105 -p 8300:8300 -p 8301:8301 -p 8302:8302 -p 8303:8303 -p 8304:8304 -p 8305:8305 -v $RESOURCEDIR:/resources -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-overlord:druid-overlord druid/cluster

  # Start Broker
  docker run -d --privileged --net druid-it-net --ip 172.172.172.8 ${COMMON_ENV} ${BROKER_ENV} ${OVERRIDE_ENV} --name druid-broker -p 5005:5005 -p 8082:8082 -p 8282:8282 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-middlemanager:druid-middlemanager --link druid-historical:druid-historical druid/cluster

  # Start Router
  docker run -d --privileged --net druid-it-net --ip 172.172.172.9 ${COMMON_ENV} ${ROUTER_ENV} ${OVERRIDE_ENV} --name druid-router -p 8888:8888 -p 5004:5004 -p 9088:9088 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

  # Start Router with permissive TLS settings (client auth enabled, no hostname verification, no revocation check)
  docker run -d --privileged --net druid-it-net --ip 172.172.172.10 ${COMMON_ENV} ${ROUTER_PERMISSIVE_TLS_ENV} ${OVERRIDE_ENV} --name druid-router-permissive-tls -p 5001:5001 -p 8889:8889 -p 9089:9089 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

  # Start Router with TLS but no client auth
  docker run -d --privileged --net druid-it-net --ip 172.172.172.11 ${COMMON_ENV} ${ROUTER_NO_CLIENT_AUTH_TLS_ENV} ${OVERRIDE_ENV} --name druid-router-no-client-auth-tls -p 5002:5002 -p 8890:8890 -p 9090:9090 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

  # Start Router with custom TLS cert checkers
  docker run -d --privileged --net druid-it-net --ip 172.172.172.12 ${COMMON_ENV} ${ROUTER_CUSTOM_CHECK_TLS_ENV} ${OVERRIDE_ENV} --hostname druid-router-custom-check-tls --name druid-router-custom-check-tls -p 5003:5003 -p 8891:8891 -p 9091:9091 -v $SHARED_DIR:/shared -v $SERVICE_SUPERVISORDS_DIR/druid.conf:$SUPERVISORDIR/druid.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster
}