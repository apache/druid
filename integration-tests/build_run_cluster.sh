#!/usr/bin/env bash

export DIR=$(cd $(dirname $0) && pwd)
export HADOOP_DOCKER_DIR=$DIR/../examples/quickstart/tutorial/hadoop/docker
export DOCKERDIR=$DIR/docker
export SERVICE_SUPERVISORDS_DIR=$DOCKERDIR/service-supervisords
export ENVIRONMENT_CONFIGS_DIR=$DOCKERDIR/environment-configs
export SHARED_DIR=${HOME}/shared
export SUPERVISORDIR=/usr/lib/druid/conf
export RESOURCEDIR=$DIR/src/test/resources

# so docker IP addr will be known during docker build
echo ${DOCKER_IP:=127.0.0.1} > $DOCKERDIR/docker_ip

if !($DRUID_INTEGRATION_TEST_SKIP_BUILD_DOCKER_CONTAINER); then
  sh ./script/copy_resources.sh
  sh ./script/docker_build_containers.sh
fi

if !($DRUID_INTEGRATION_TEST_SKIP_RUN_DOCKER_CONTAINER); then
  sh ./stop_cluster.sh
  sh ./script/docker_run_cluster.sh
fi

if ($DRUID_INTEGRATION_TEST_SKIP_RUN_DOCKER_CONTAINER) && ($DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER)
then
  sh ./script/copy_hadoop_resources.sh
fi