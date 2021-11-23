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
mvn -B dependency:copy-dependencies -DoutputDirectory=$SHARED_DIR/docker/lib

# Make directories if they dont exist
mkdir -p $SHARED_DIR/hadoop_xml
mkdir -p $SHARED_DIR/hadoop-dependencies
mkdir -p $SHARED_DIR/logs
mkdir -p $SHARED_DIR/tasklogs
mkdir -p $SHARED_DIR/docker/extensions
mkdir -p $SHARED_DIR/docker/credentials

# install logging config
cp src/main/resources/log4j2.xml $SHARED_DIR/docker/lib/log4j2.xml

# copy the integration test jar, it provides test-only extension implementations
cp target/druid-integration-tests*.jar $SHARED_DIR/docker/lib

# move extensions into a seperate extension folder
# For druid-integration-tests
mkdir -p $SHARED_DIR/docker/extensions/druid-integration-tests
# We don't want to copy tests jar.
cp $SHARED_DIR/docker/lib/druid-integration-tests-*[^s].jar $SHARED_DIR/docker/extensions/druid-integration-tests
# For druid-s3-extensions
mkdir -p $SHARED_DIR/docker/extensions/druid-s3-extensions
mv $SHARED_DIR/docker/lib/druid-s3-extensions-* $SHARED_DIR/docker/extensions/druid-s3-extensions
# For druid-azure-extensions
mkdir -p $SHARED_DIR/docker/extensions/druid-azure-extensions
mv $SHARED_DIR/docker/lib/druid-azure-extensions-* $SHARED_DIR/docker/extensions/druid-azure-extensions
# For druid-google-extensions
mkdir -p $SHARED_DIR/docker/extensions/druid-google-extensions
mv $SHARED_DIR/docker/lib/druid-google-extensions-* $SHARED_DIR/docker/extensions/druid-google-extensions
# For druid-hdfs-storage
mkdir -p $SHARED_DIR/docker/extensions/druid-hdfs-storage
mv $SHARED_DIR/docker/lib/druid-hdfs-storage-* $SHARED_DIR/docker/extensions/druid-hdfs-storage
# For druid-kinesis-indexing-service
mkdir -p $SHARED_DIR/docker/extensions/druid-kinesis-indexing-service
mv $SHARED_DIR/docker/lib/druid-kinesis-indexing-service-* $SHARED_DIR/docker/extensions/druid-kinesis-indexing-service
# For druid-parquet-extensions
# Using cp so that this extensions is included when running Druid without loadList and as a option for the loadList
mkdir -p $SHARED_DIR/docker/extensions/druid-parquet-extensions
cp $SHARED_DIR/docker/lib/druid-parquet-extensions-* $SHARED_DIR/docker/extensions/druid-parquet-extensions
# For druid-orc-extensions
# Using cp so that this extensions is included when running Druid without loadList and as a option for the loadList
mkdir -p $SHARED_DIR/docker/extensions/druid-orc-extensions
cp $SHARED_DIR/docker/lib/druid-orc-extensions-* $SHARED_DIR/docker/extensions/druid-orc-extensions

# Pull Hadoop dependency if needed
if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
then
  ## We put same version in both commands but as we have an if, correct code path will always be executed as this is generated script.
  ## <TODO> Remove if
  if [ -n "${HADOOP_VERSION}" ] && [ "${HADOOP_VERSION:0:1}" == "3" ]; then
    java -cp "$SHARED_DIR/docker/lib/*" -Ddruid.extensions.hadoopDependenciesDir="$SHARED_DIR/hadoop-dependencies" org.apache.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client-api:${hadoop.compile.version} -h org.apache.hadoop:hadoop-client-runtime:${hadoop.compile.version} -h org.apache.hadoop:hadoop-aws:${hadoop.compile.version} -h org.apache.hadoop:hadoop-azure:${hadoop.compile.version}
    curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --output $SHARED_DIR/docker/lib/gcs-connector-hadoop3-latest.jar
  else
    java -cp "$SHARED_DIR/docker/lib/*" -Ddruid.extensions.hadoopDependenciesDir="$SHARED_DIR/hadoop-dependencies" org.apache.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client:${hadoop.compile.version} -h org.apache.hadoop:hadoop-aws:${hadoop.compile.version} -h org.apache.hadoop:hadoop-azure:${hadoop.compile.version}
    curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar --output $SHARED_DIR/docker/lib/gcs-connector-hadoop2-latest.jar
  fi
fi

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
