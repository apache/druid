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

echo "Copying integration test resources."

set -e

# setup client keystore
./docker/tls/generate-client-certs-and-keystores.sh
rm -rf docker/client_tls
cp -r client_tls docker/client_tls

# install druid jars
rm -rf $SHARED_DIR/docker
mkdir -p $SHARED_DIR
cp -R docker $SHARED_DIR/docker

if [ -z "$DRUID_INTEGRATION_TEST_SKIP_MAVEN_BUILD_DOCKER" ] || [ "$DRUID_INTEGRATION_TEST_SKIP_MAVEN_BUILD_DOCKER" = "false" ]
then
  pushd ../
  rm -rf distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin
  # using parallel build here may not yield significant speedups
  mvn -B -Pskip-static-checks -DskipTests -Dweb.console.skip=${DRUID_INTEGRATION_TEST_SKIP_WEB_CONSOLE:-true} install -Pintegration-test
  mv distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/bin $SHARED_DIR/docker/bin
  mv distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/lib $SHARED_DIR/docker/lib
  mv distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/extensions $SHARED_DIR/docker/extensions
  popd
else
  cp -R ../distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/bin $SHARED_DIR/docker/bin
  cp -R ../distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/lib $SHARED_DIR/docker/lib
  cp -R ../distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/extensions $SHARED_DIR/docker/extensions
fi

# Make directoriess if they dont exist
mkdir -p $SHARED_DIR/hadoop_xml
mkdir -p $SHARED_DIR/hadoop-dependencies
mkdir -p $SHARED_DIR/logs
mkdir -p $SHARED_DIR/tasklogs
mkdir -p $SHARED_DIR/docker/credentials

# install logging config
cp src/main/resources/log4j2.xml $SHARED_DIR/docker/lib/log4j2.xml

# Extensions for testing are pulled while creating a binary.
# See the 'integration-test' profile in $ROOT/distribution/pom.xml.


# one of the integration tests needs the wikiticker sample data
mkdir -p $SHARED_DIR/wikiticker-it
cp ../examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz $SHARED_DIR/wikiticker-it/wikiticker-2015-09-12-sampled.json.gz
cp docker/wiki-simple-lookup.json $SHARED_DIR/wikiticker-it/wiki-simple-lookup.json
cp docker/test-data/wikipedia.desc $SHARED_DIR/wikiticker-it/wikipedia.desc

# copy other files if needed
if [ -n "$DRUID_INTEGRATION_TEST_RESOURCE_FILE_DIR_PATH" ]
then
  cp -a $DRUID_INTEGRATION_TEST_RESOURCE_FILE_DIR_PATH/. $SHARED_DIR/docker/credentials/
fi
