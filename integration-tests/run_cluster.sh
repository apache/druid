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

# cleanup
for node in druid-historical druid-coordinator druid-overlord druid-router druid-router-permissive-tls druid-router-no-client-auth-tls druid-router-custom-check-tls druid-broker druid-middlemanager druid-zookeeper-kafka druid-metadata-storage;
do
docker stop $node
docker rm $node
done

docker network rm druid-it-net

# environment variables
DIR=$(cd $(dirname $0) && pwd)
DOCKERDIR=$DIR/docker
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
mkdir -p $SHARED_DIR/logs
mkdir -p $SHARED_DIR/tasklogs

# install druid jars 
rm -rf $SHARED_DIR/docker
cp -R docker $SHARED_DIR/docker
mvn -B dependency:copy-dependencies -DoutputDirectory=$SHARED_DIR/docker/lib

# install logging config
cp src/main/resources/log4j2.xml $SHARED_DIR/docker/lib/log4j2.xml

# copy the integration test jar, it provides test-only extension implementations
cp target/druid-integration-tests*.jar $SHARED_DIR/docker/lib

# one of the integration tests needs the wikiticker sample data
mkdir -p $SHARED_DIR/wikiticker-it
cp ../examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz $SHARED_DIR/wikiticker-it/wikiticker-2015-09-12-sampled.json.gz
cp docker/wiki-simple-lookup.json $SHARED_DIR/wikiticker-it/wiki-simple-lookup.json

docker network create --subnet=172.172.172.0/24 druid-it-net

# Build Druid Cluster Image
docker build -t druid/cluster $SHARED_DIR/docker

# Start zookeeper and kafka
docker run -d --privileged --net druid-it-net --ip 172.172.172.2 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-zookeeper-kafka -p 2181:2181 -p 9092:9092 -p 9093:9093 -v $SHARED_DIR:/shared -v $DOCKERDIR/zookeeper.conf:$SUPERVISORDIR/zookeeper.conf -v $DOCKERDIR/kafka.conf:$SUPERVISORDIR/kafka.conf druid/cluster

# Start MYSQL 
docker run -d --privileged --net druid-it-net --ip 172.172.172.3 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-metadata-storage -v $SHARED_DIR:/shared -v $DOCKERDIR/metadata-storage.conf:$SUPERVISORDIR/metadata-storage.conf druid/cluster

# Start Overlord
docker run -d --privileged --net druid-it-net --ip 172.172.172.4 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-overlord -p 8090:8090 -p 8290:8290 -v $SHARED_DIR:/shared -v $DOCKERDIR/overlord.conf:$SUPERVISORDIR/overlord.conf --link druid-metadata-storage:druid-metadata-storage --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

# Start Coordinator
docker run -d --privileged --net druid-it-net --ip 172.172.172.5 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-coordinator -p 8081:8081 -p 8281:8281 -v $SHARED_DIR:/shared -v $DOCKERDIR/coordinator.conf:$SUPERVISORDIR/coordinator.conf --link druid-overlord:druid-overlord --link druid-metadata-storage:druid-metadata-storage --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

# Start Historical 
docker run -d --privileged --net druid-it-net --ip 172.172.172.6 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-historical -p 8083:8083 -p 8283:8283 -v $SHARED_DIR:/shared -v $DOCKERDIR/historical.conf:$SUPERVISORDIR/historical.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka druid/cluster

# Start Middlemanger
docker run -d --privileged --net druid-it-net --ip 172.172.172.7 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-middlemanager -p 8091:8091 -p 8291:8291 -p 8100:8100 -p 8101:8101 -p 8102:8102 -p 8103:8103 -p 8104:8104 -p 8105:8105 -p 8300:8300 -p 8301:8301 -p 8302:8302 -p 8303:8303 -p 8304:8304 -p 8305:8305 -v $RESOURCEDIR:/resources -v $SHARED_DIR:/shared -v $DOCKERDIR/middlemanager.conf:$SUPERVISORDIR/middlemanager.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-overlord:druid-overlord druid/cluster

# Start Broker 
docker run -d --privileged --net druid-it-net --ip 172.172.172.8 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-broker -p 8082:8082 -p 8282:8282 -v $SHARED_DIR:/shared -v $DOCKERDIR/broker.conf:$SUPERVISORDIR/broker.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-middlemanager:druid-middlemanager --link druid-historical:druid-historical druid/cluster

# Start Router
docker run -d --privileged --net druid-it-net --ip 172.172.172.9 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-router -p 8888:8888 -p 9088:9088 -v $SHARED_DIR:/shared -v $DOCKERDIR/router.conf:$SUPERVISORDIR/router.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

# Start Router with permissive TLS settings (client auth enabled, no hostname verification, no revocation check)
docker run -d --privileged --net druid-it-net --ip 172.172.172.10 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-router-permissive-tls -p 8889:8889 -p 9089:9089 -v $SHARED_DIR:/shared -v $DOCKERDIR/router-permissive-tls.conf:$SUPERVISORDIR/router-permissive-tls.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

# Start Router with TLS but no client auth
docker run -d --privileged --net druid-it-net --ip 172.172.172.11 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --name druid-router-no-client-auth-tls -p 8890:8890 -p 9090:9090 -v $SHARED_DIR:/shared -v $DOCKERDIR/router-no-client-auth-tls.conf:$SUPERVISORDIR/router-no-client-auth-tls.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster

# Start Router with custom TLS cert checkers
docker run -d --privileged --net druid-it-net --ip 172.172.172.12 -e LANG=C.UTF-8 -e LANGUAGE=C.UTF-8 -e LC_ALL=C.UTF-8 --hostname druid-router-custom-check-tls --name druid-router-custom-check-tls -p 8891:8891 -p 9091:9091 -v $SHARED_DIR:/shared -v $DOCKERDIR/router-custom-check-tls.conf:$SUPERVISORDIR/router-custom-check-tls.conf --link druid-zookeeper-kafka:druid-zookeeper-kafka --link druid-coordinator:druid-coordinator --link druid-broker:druid-broker druid/cluster
