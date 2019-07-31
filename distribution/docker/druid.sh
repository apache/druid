#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# NOTE: this is a 'run' script for the stock tarball
# It takes 1 required argument (the name of the service,
# e.g. 'broker', 'historical' etc). Any additional arguments
# are passed to that service.
#
# It accepts 'JAVA_OPTS' as an environment variable
#
# Additional env vars:
# - DRUID_LOG4J -- set the entire log4j.xml verbatim
# - DRUID_LOG_LEVEL -- override the default log level in default log4j
# - DRUID_XMX -- set Java Xmx
# - DRUID_XMS -- set Java Xms
# - DRUID_MAXNEWSIZE -- set Java max new size
# - DRUID_NEWSIZE -- set Java new size
# - DRUID_MAXDIRECTMEMORYSIZE -- set Java max direct memory size
#
# - DRUID_CONFIG -- full path to a file for druid 'common' properties
# - DRUID_CONFIG_${service} -- full path to a file for druid 'service' properties

set -e
SERVICE="$1"

echo "$(date -Is) startup service $SERVICE"

# We put all the config in /tmp/conf to allow for a
# read-only root filesystem
cp -r /opt/druid/conf /tmp/conf

# Delete the old key (if existing) and append new key=value
setKey() {
    service="$1"
    key="$2"
    value="$3"
    case "$service" in
        _common)
          fname=common.runtime.properties ;;
        *)
          fname=runtime.properties ;;
    esac
    # Delete from all
    sed -ri "/$key=/d" /tmp/conf/druid/cluster/_common/common.runtime.properties
    [ -f /tmp/conf/druid/cluster/$service/$fname ] && sed -ri "/$key=/d" /tmp/conf/druid/cluster/$service/$fname
    [ -f /tmp/conf/druid/cluster/$service/$fname ] && echo "$key=$value" >> /tmp/conf/druid/cluster/$service/$fname
    [ -f /tmp/conf/druid/cluster/$service/$fname ] || echo "$key=$value" >> /tmp/conf/druid/cluster/_common/$fname
}

setJavaKey() {
    service="$1"
    key=$2
    value=$3
    file=/tmp/conf/druid/cluster/$service/jvm.config
    sed -ri "/$key/d" $file
    echo $value >> $file
}

## Setup host names
if [ -n "${ZOOKEEPER}" ]
then
    setKey _common druid.zk.service.host "${ZOOKEEPER}"
fi

setKey $SERVICE druid.host $(ip r get 1 | awk '{print $7;exit}')


env |grep ^druid_ | while read evar
do
    # Can't use IFS='=' to parse since var might have = in it (e.g. password)
    val=$(echo "$evar" | sed -e 's?[^=]*=??')
    var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?_?.?g')
    setKey $SERVICE "$var" "$val"
done

env |grep ^s3service | while read evar
do
    val=$(echo "$evar" | sed -e 's?[^=]*=??')
    var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?_?.?' -e 's?_?-?g')
    echo "$var=$val" >> /tmp/conf/druid/cluster/_common/jets3t.properties
done

# This is to allow configuration via a Kubernetes configMap without
# e.g. using subPath (you can also mount the configMap on /tmp/conf/druid)
if [ -n "$DRUID_CONFIG_COMMON" ]
then
    cp -f "$DRUID_CONFIG_COMMON" /tmp/conf/druid/cluster/_common/common.runtime.properties
fi

SCONFIG=$(printf "%s_%s" DRUID_CONFIG ${SERVICE})
SCONFIG=$(eval echo \$$(echo $SCONFIG))

if [ -n "${SCONFIG}" ]
then
    cp -f "${SCONFIG}" /tmp/conf/druid/cluster/${SERVICE}/runtime.properties
fi

# Now do the java options

if [ -n "$DRUID_XMX" ]; then setJavaKey ${SERVICE} -Xmx -Xmx${DRUID_XMX}; fi
if [ -n "$DRUID_XMS" ]; then setJavaKey ${SERVICE} -Xms -Xms${DRUID_XMS}; fi
if [ -n "$DRUID_MAXNEWSIZE" ]; then setJavaKey ${SERVICE} -XX:MaxNewSize -XX:MaxNewSize=${DRUID_MAXNEWSIZE}; fi
if [ -n "$DRUID_NEWSIZE" ]; then setJavaKey ${SERVICE} -XX:NewSize -XX:MaxNewSize=${DRUID_NEWSIZE}; fi
if [ -n "$DRUID_MAXDIRECTMEMORYSIZE" ]; then setJavaKey ${SERVICE} -XX:MaxDirectMemorySize -XX:MaxDirectMemorySize=${DRUID_MAXDIRECTMEMORYSIZE}; fi

JAVA_OPTS="$JAVA_OPTS $(cat /tmp/conf/druid/cluster/${SERVICE}/jvm.config | xargs)"

if [ -n "$DRUID_LOG_LEVEL" ]
then
    sed -ri 's/"info"/"'$DRUID_LOG_LEVEL'"/g' /tmp/conf/druid/cluster/_common/log4j2.xml
fi

if [ -n "$DRUID_LOG4J" ]
then
    echo "$DRUID_LOG4J" > /tmp/conf/druid/cluster/_common/log4j2.xml
fi

mkdir -p var/tmp var/druid/segments var/druid/indexing-logs var/druid/task var/druid/hadoop-tmp var/druid/segment-cache
exec java ${JAVA_OPTS} -cp /tmp/conf/druid/cluster/_common:/tmp/conf/druid/cluster/${SERVICE}:lib/*: org.apache.druid.cli.Main server $@
