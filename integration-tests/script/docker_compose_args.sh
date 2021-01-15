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

# picks appropriate docker-compose argments to use when bringing up and down integration test clusters
# for a given test group
getComposeArgs()
{
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
      if [ "$DRUID_INTEGRATION_TEST_GROUP" = "security" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "query-retry" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "high-availability" ]
      then
        echo "Cannot run test group '$DRUID_INTEGRATION_TEST_GROUP' with CliIndexer"
        exit 1
      fi

      # Replace MiddleManager with Indexer
      echo "-f ${DOCKERDIR}/docker-compose.cli-indexer.yml"
    elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "security" ]
    then
      # default + additional druid router (custom-check-tls, permissive-tls, no-client-auth-tls)
      echo "-f ${DOCKERDIR}/docker-compose.yml -f ${DOCKERDIR}/docker-compose.security.yml"
    elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "query-retry" ]
    then
      # default + additional historical modified for query retry test
      # See CliHistoricalForQueryRetryTest.
      echo "-f ${DOCKERDIR}/docker-compose.query-retry-test.yml"
    elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "high-availability" ]
    then
      # the 'high availability' test cluster with multiple coordinators and overlords
      echo "-f ${DOCKERDIR}/docker-compose.high-availability.yml"
    else
      # default
      echo "-f ${DOCKERDIR}/docker-compose.yml"
    fi
  else
    # with override config
    echo "-f ${DOCKERDIR}/docker-compose.override-env.yml"
  fi
}
