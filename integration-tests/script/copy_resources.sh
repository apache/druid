#!/usr/bin/env bash

# setup client keystore
./docker/tls/generate-client-certs-and-keystores.sh
rm -rf docker/client_tls
cp -r client_tls docker/client_tls

# Make directories if they dont exist
mkdir -p $SHARED_DIR/hadoop_xml
mkdir -p $SHARED_DIR/hadoop-dependencies
mkdir -p $SHARED_DIR/logs
mkdir -p $SHARED_DIR/tasklogs
mkdir -p $SHARED_DIR/docker/extensions
mkdir -p $SHARED_DIR/docker/credentials

# install druid jars
rm -rf $SHARED_DIR/docker
cp -R docker $SHARED_DIR/docker
mvn -B dependency:copy-dependencies -DoutputDirectory=$SHARED_DIR/docker/lib

# move extensions into a seperate extension folder
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

# Pull Hadoop dependency if needed
if [ -n "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" ] && [ "$DRUID_INTEGRATION_TEST_START_HADOOP_DOCKER" == true ]
then
  java -cp "$SHARED_DIR/docker/lib/*" -Ddruid.extensions.hadoopDependenciesDir="$SHARED_DIR/hadoop-dependencies" org.apache.druid.cli.Main tools pull-deps -h org.apache.hadoop:hadoop-client:2.8.5 -h org.apache.hadoop:hadoop-aws:2.8.5
fi

# install logging config
cp src/main/resources/log4j2.xml $SHARED_DIR/docker/lib/log4j2.xml

# copy the integration test jar, it provides test-only extension implementations
cp target/druid-integration-tests*.jar $SHARED_DIR/docker/lib

# one of the integration tests needs the wikiticker sample data
mkdir -p $SHARED_DIR/wikiticker-it
cp ../examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz $SHARED_DIR/wikiticker-it/wikiticker-2015-09-12-sampled.json.gz
cp docker/wiki-simple-lookup.json $SHARED_DIR/wikiticker-it/wiki-simple-lookup.json

# copy other files if needed
if [ -n "$DRUID_INTEGRATION_TEST_RESOURCE_FILE_DIR_PATH" ]
then
  cp -a $DRUID_INTEGRATION_TEST_RESOURCE_FILE_DIR_PATH/. $SHARED_DIR/docker/credentials/
fi
