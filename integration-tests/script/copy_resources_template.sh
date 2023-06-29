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

pushd ../
rm -rf distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin
mvn -P skip-static-checks,skip-tests -T1C -Danimal.sniffer.skip=true -Dcheckstyle.skip=true -Dweb.console.skip=true -Denforcer.skip=true -Dforbiddenapis.skip=true -Dmaven.javadoc.skip=true -Dpmd.skip=true -Dspotbugs.skip=true install -Pintegration-test
mv distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/lib $SHARED_DIR/docker/lib
mv distribution/target/apache-druid-$DRUID_VERSION-integration-test-bin/extensions $SHARED_DIR/docker/extensions
popd

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

# Pull Hadoop dependency if needed
if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
then
  # HdfsStorageDruidModule loads all implementations of org.apache.hadoop.fs.FileSystem using an extension class loader.
  # This requires for all FileSystem implementations to be installed in druid-hdfs-storage.
  DRUID_HDFS_EXT=$SHARED_DIR/docker/extensions/druid-hdfs-storage
  HADOOP_AWS_DIR=$SHARED_DIR/hadoop-dependencies/hadoop-aws/${hadoop.compile.version}
  HADOOP_GCS_DIR=$SHARED_DIR/hadoop-dependencies/hadoop-gcs/${hadoop.compile.version}
  HADOOP_AZURE_DIR=$SHARED_DIR/hadoop-dependencies/hadoop-azure/${hadoop.compile.version}
  mkdir -p $DRUID_HDFS_EXT
  mkdir -p $HADOOP_GCS_DIR
  ## We put same version in both commands but as we have an if, correct code path will always be executed as this is generated script.
  ## <TODO> Remove if
  if [ -n "${HADOOP_VERSION}" ] && [ "${HADOOP_VERSION:0:1}" == "3" ]; then
    java -cp "$SHARED_DIR/docker/lib/*" -Ddruid.extensions.hadoopDependenciesDir="$SHARED_DIR/hadoop-dependencies" org.apache.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client-api:${hadoop.compile.version} -h org.apache.hadoop:hadoop-client-runtime:${hadoop.compile.version} -h org.apache.hadoop:hadoop-aws:${hadoop.compile.version} -h org.apache.hadoop:hadoop-azure:${hadoop.compile.version}
    curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --output $HADOOP_GCS_DIR/gcs-connector-hadoop3-latest.jar
    cp $HADOOP_GCS_DIR/gcs-connector-hadoop3-latest.jar $DRUID_HDFS_EXT
  else
    java -cp "$SHARED_DIR/docker/lib/*" -Ddruid.extensions.hadoopDependenciesDir="$SHARED_DIR/hadoop-dependencies" org.apache.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client:${hadoop.compile.version} -h org.apache.hadoop:hadoop-aws:${hadoop.compile.version} -h org.apache.hadoop:hadoop-azure:${hadoop.compile.version}
    curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar --output $HADOOP_GCS_DIR/gcs-connector-hadoop2-latest.jar
    cp $HADOOP_GCS_DIR/gcs-connector-hadoop2-latest.jar $DRUID_HDFS_EXT
  fi
  cp $HADOOP_AWS_DIR/hadoop-aws-${hadoop.compile.version}.jar $DRUID_HDFS_EXT
  cp $HADOOP_AZURE_DIR/hadoop-azure-${hadoop.compile.version}.jar $DRUID_HDFS_EXT
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
