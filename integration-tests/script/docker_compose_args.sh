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

# picks appropriate docker-compose arguments to use when bringing up and down integration test clusters
# for a given test group
getComposeArgs()
{
  # Sanity check: DRUID_INTEGRATION_TEST_INDEXER must be "indexer" or "middleManager"
  if [ "$DRUID_INTEGRATION_TEST_INDEXER" != "indexer" ] && [ "$DRUID_INTEGRATION_TEST_INDEXER" != "middleManager" ]
  then
    echo "DRUID_INTEGRATION_TEST_INDEXER must be 'indexer' or 'middleManager' (is '$DRUID_INTEGRATION_TEST_INDEXER')"
    exit 1
  fi
  if [ "$DRUID_INTEGRATION_TEST_INDEXER" = "indexer" ]
  then
    # Sanity check: cannot combine CliIndexer tests with security tests
    if [ "$DRUID_INTEGRATION_TEST_GROUP" = "security" ]
    then
      echo "Cannot run test group '$DRUID_INTEGRATION_TEST_GROUP' with CliIndexer"
      exit 1
    else
      # Replace MiddleManager with Indexer
      echo "-f ${DOCKERDIR}/docker-compose.cli-indexer.yml"
    fi
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "security" ]
  then
    # default + additional druid router (custom-check-tls, permissive-tls, no-client-auth-tls)
    echo "-f ${DOCKERDIR}/docker-compose.yml -f ${DOCKERDIR}/docker-compose.security.yml"
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "kinesis-data-format" ]
      then
        # default + with override config + schema registry container
        echo "-f ${DOCKERDIR}/docker-compose.yml -f ${DOCKERDIR}/docker-compose.schema-registry.yml"
  elif [ "$DRUID_INTEGRATION_TEST_GROUP" = "centralized-datasource-schema" ]
      then
        # cluster with overriden properties for broker and coordinator
        echo "-f ${DOCKERDIR}/docker-compose.centralized-datasource-schema.yml"
  else
    # default
    echo "-f ${DOCKERDIR}/docker-compose.yml"
  fi
}
