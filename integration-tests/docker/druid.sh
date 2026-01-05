#!/bin/bash

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

getConfPath()
{
    cluster_conf_base=/tmp/conf/druid/cluster
    case "$1" in
    _common) echo $cluster_conf_base/_common ;;
    historical-for-query-error-test) echo $cluster_conf_base/data/historical ;;
    historical) echo $cluster_conf_base/data/historical ;;
    middleManager) echo $cluster_conf_base/data/middleManager ;;
    indexer) echo $cluster_conf_base/data/indexer ;;
    coordinator) echo $cluster_conf_base/master/coordinator ;;
    broker) echo $cluster_conf_base/query/broker ;;
    router) echo $cluster_conf_base/query/router ;;
    overlord) echo $cluster_conf_base/master/overlord ;;
    *) echo $cluster_conf_base/misc/$1 ;;
    esac
}

# Delete the old key (if existing) and append new key=value
setKey()
{
    service="$1"
    key="$2"
    value="$3"
    service_conf=$(getConfPath $service)/runtime.properties
    # Delete from all
    sed -ri "/$key=/d" $COMMON_CONF_DIR/common.runtime.properties
    [ -f $service_conf ] && sed -ri "/$key=/d" $service_conf
    [ -f $service_conf ] && echo "$key=$value" >>$service_conf
    [ -f $service_conf ] || echo "$key=$value" >>$COMMON_CONF_DIR/common.runtime.properties

    echo "Setting $key=$value in $service_conf"
}

setupConfig()
{
  echo "$(date -Is) configuring service $DRUID_SERVICE"

  # We put all the config in /tmp/conf to allow for a
  # read-only root filesystem
  mkdir -p /tmp/conf/druid

  COMMON_CONF_DIR=$(getConfPath _common)
  SERVICE_CONF_DIR=$(getConfPath ${DRUID_SERVICE})

  mkdir -p $COMMON_CONF_DIR
  mkdir -p $SERVICE_CONF_DIR
  touch $COMMON_CONF_DIR/common.runtime.properties
  touch $SERVICE_CONF_DIR/runtime.properties

  setKey $DRUID_SERVICE druid.host $(resolveip -s $HOSTNAME)
  setKey $DRUID_SERVICE druid.worker.ip $(resolveip -s $HOSTNAME)

  # Write out all the environment variables starting with druid_ to druid service config file
  # This will replace _ with . in the key
  env | grep ^druid_ | while read evar;
  do
      # Can't use IFS='=' to parse since var might have = in it (e.g. password)
      val=$(echo "$evar" | sed -e 's?[^=]*=??')
      var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?_?.?g')
      setKey $DRUID_SERVICE "$var" "$val"
  done

  setKey $DRUID_SERVICE druid.metadata.mysql.driver.driverClassName $MYSQL_DRIVER_CLASSNAME
}

setupData()
{
  # note: this function exists for legacy reasons, ideally we should do data insert in IT's setup method.
  if [ ! -f /usr/lib/druid/conf/metadata-storage.conf ]; then
    # Only metadata storage server need to set up data.
    echo "Skip setup data. Metadata store is not configured."
    return
  fi

  service mariadb start
  for i in {30..0}; do
    mysqladmin ping --silent && break
    sleep 1
  done
  if [ "$i" = 0 ]; then
    echo "MySQL did not start"
    exit 1
  fi
  # The "query" and "security" test groups require data to be setup before running the tests.
  # In particular, they requires segments to be download from a pre-existing s3 bucket.
  # This is done by using the loadSpec put into metadatastore and s3 credientials set below.
  if [ "$DRUID_INTEGRATION_TEST_GROUP" = "query" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "security" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "upgrade" ] || [ "$DRUID_INTEGRATION_TEST_GROUP" = "centralized-datasource-schema" ]; then
    cat /test-data/${DRUID_INTEGRATION_TEST_GROUP}-sample-data.sql | mysql -u root druid
  fi

  service mariadb stop
}
