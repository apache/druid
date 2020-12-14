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

set -e

# Create docker network
{
  docker network create --subnet=172.172.172.0/24 druid-it-net
}

if [ -z "$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH" ]
then
    echo "\$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH is not set. No override config file provided"
    if [ "$DRUID_INTEGRATION_TEST_GROUP" = "s3-deep-storage" ] || \
    [ "$DRUID_INTEGRATION_TEST_GROUP" = "gcs-deep-storage" ] || \
    [ "$DRUID_INTEGRATION_TEST_GROUP" = "azure-deep-storage" ] || \
    [ "$DRUID_INTEGRATION_TEST_GROUP" = "hdfs-deep-storage" ] || \
    [ "$DRUID_INTEGRATION_TEST_GROUP" = "s3-ingestion" ] || \
    [ "$DRUID_INTEGRATION_TEST_GROUP" = "kinesis-index" ] || \
    [ "$DRUID_INTEGRATION_TEST_GROUP" = "kinesis-data-format" ]; then
      echo "Test group $DRUID_INTEGRATION_TEST_GROUP requires override config file. Stopping test..."
      exit 1
    fi
else
    echo "\$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH is set with value ${DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH}"
fi

# Start docker containers for all Druid processes and dependencies
{
  # Start Hadoop docker if needed
  if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
  then
    # Start Hadoop docker container
    docker-compose -f ${DOCKERDIR}/docker-compose.druid-hadoop.yml up -d
  fi

  # Start Druid services
  if [ -z "$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH" ]
  then
    # Sanity check: DRUID_INTEGRATION_TEST_INDEXER must be "indexer" or "middleManager"
    if [ "$DRUID_INTEGRATION_TEST_INDEXER" != "indexer" ] && [ "$DRUID_INTEGRATION_TEST_INDEXER" != "middleManager" ]
    then
      echo "DRUID_INTEGRATION_TEST_INDEXER must be 'indexer' or 'middleManager' (is '$DRUID_INTEGRATION_TEST_INDEXER')"
      exit 1
    fi

    if [ "$DRUID_INTEGRATION_TEST_INDEXER" = "indexer" ]
    then
      # Sanity check: cannot combine CliIndexer tests with security, query-retry tests
      if [ "$DRUID_INTEGRATION_TEST_GROUP" = "security" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "query-retry" ]
      then
        echo "Cannot run test group '$DRUID_INTEGRATION_TEST_GROUP' with CliIndexer"
        exit 1
      fi

      # Replace MiddleManager with Indexer
      docker-compose -f ${DOCKERDIR}/docker-compose.cli-indexer.yml up -d
    elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "security" ]
    then
      # Start default Druid services and additional druid router (custom-check-tls, permissive-tls, no-client-auth-tls)
      docker-compose -f ${DOCKERDIR}/docker-compose.yml -f ${DOCKERDIR}/docker-compose.security.yml up -d
    elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "query-retry" ]
    then
      # Start default Druid services with an additional historical modified for query retry test
      # See CliHistoricalForQueryRetryTest.
      docker-compose -f ${DOCKERDIR}/docker-compose.query-retry-test.yml up -d
    else
      # Start default Druid services
      docker-compose -f ${DOCKERDIR}/docker-compose.yml up -d
    fi
  else
    # run druid cluster with override config
    OVERRIDE_ENV=$DRUID_INTEGRATION_TEST_OVERRIDE_CONFIG_PATH docker-compose -f ${DOCKERDIR}/docker-compose.override-env.yml up -d
  fi
}
