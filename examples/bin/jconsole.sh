#!/bin/bash
############################
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
############################
# This script downloads the appropriate log4j2 jars and runs jconsole with them as plugins.
# This script can be used as an example for how to connect to a Druid instance to
# change the logging parameters at runtime
############################

LOG4J2_VERSION=2.4.1
MVN_REPO=`mvn help:evaluate -Dexpression=settings.localRepository | grep -v '\[INFO\]' | tail -n 1`
LOG4J_API_PATH=${MVN_REPO}/org/apache/logging/log4j/log4j-api/${LOG4J2_VERSION}/log4j-api-${LOG4J2_VERSION}.jar
LOG4J_CORE_PATH=${MVN_REPO}/org/apache/logging/log4j/log4j-core/${LOG4J2_VERSION}/log4j-core-${LOG4J2_VERSION}.jar
LOG4J_GUI_PATH=${MVN_REPO}/org/apache/logging/log4j/log4j-jmx-gui/${LOG4J2_VERSION}/log4j-jmx-gui-${LOG4J2_VERSION}.jar
APACHE_REPO='https://repository.apache.org/content/groups/public'

if [ ! -e ${LOG4J_API_PATH} ]; then
	echo "Downloading missing jars for Log4j api version ${LOG4J2_VERSION} to ${LOG4J_API_PATH}"
	mvn dependency:get -DrepoUrl=${APACHE_REPO} -Dartifact=org.apache.logging.log4j:log4j-api:${LOG4J2_VERSION} > /dev/null 2>&1 &
fi
if [ ! -e ${LOG4J_CORE_PATH} ]; then
	echo "Downloading missing jars for Log4j core version ${LOG4J2_VERSION} to ${LOG4J_CORE_PATH}"
	mvn dependency:get -DrepoUrl=${APACHE_REPO} -Dartifact=org.apache.logging.log4j:log4j-core:${LOG4J2_VERSION} > /dev/null 2>&1 &
fi
if [ ! -e ${LOG4J_GUI_PATH} ]; then
	echo "Downloading missing jars for Log4j gui version ${LOG4J2_VERSION} to ${LOG4J_GUI_PATH}"
	mvn dependency:get -DrepoUrl=${APACHE_REPO} -Dartifact=org.apache.logging.log4j:log4j-jmx-gui:${LOG4J2_VERSION} > /dev/null 2>&1 &
fi
wait

WHEREAMI="$(dirname "$0")"
WHEREAMI="$(cd "$WHEREAMI" && pwd)"
JAVA_BIN_DIR="$(source "$WHEREAMI"/java-util && get_java_bin_dir)"
if [ -z "$JAVA_BIN_DIR" ]; then
  >&2 echo "Could not find java - please run $WHEREAMI/verify-java to confirm it is installed."
  exit 1
fi
"$JAVA_BIN_DIR"/jconsole -pluginpath ${LOG4J_API_PATH}:${LOG4J_CORE_PATH}:${LOG4J_GUI_PATH} $@
