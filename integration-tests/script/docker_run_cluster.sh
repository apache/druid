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
    docker-compose -f ${DOCKERDIR}/druid-hadoop.yml up -d

    sh ./copy_hadoop_resources.sh
  fi

  if [ -z "$OVERRIDE_ENV" ]
  then
     docker-compose -f ${DOCKERDIR}/docker-compose.yml up -d
  else
    # run druid cluster with override config
    docker-compose -f ${DOCKERDIR}/docker-compose-override-env.yml up -d
  fi
}
