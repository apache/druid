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

echo $DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH

export DIR=$(cd $(dirname $0) && pwd)
export HADOOP_DOCKER_DIR=$DIR/../examples/quickstart/tutorial/hadoop/docker
export DOCKERDIR=$DIR/docker
export SHARED_DIR=${HOME}/shared

# so docker IP addr will be known during docker build
echo ${DOCKER_IP:=127.0.0.1} > $DOCKERDIR/docker_ip

if !($DRUID_INTEGRATION_TEST_SKIP_BUILD_DOCKER); then
  bash ./script/copy_resources.sh
  bash ./script/docker_build_containers.sh
fi

if !($DRUID_INTEGRATION_TEST_SKIP_RUN_DOCKER); then
  bash ./stop_cluster.sh
  bash ./script/docker_run_cluster.sh
fi

if ($DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER); then
  bash ./script/copy_hadoop_resources.sh
fi
