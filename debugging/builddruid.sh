#!/bin/bash
set -e
cd ../

# mvn clean install -DskipTests=true -pl :druid-server
# mvn clean install -DskipTests=true -pl :druid-pubsub-indexing-service
# mvn clean install -DskipTests=true -pl :druid-sql
# mvn clean install -DskipTests=true -pl :druid-basic-security

mvn clean install -Pdist -DskipTests=true  # -pl :distribution
# mvn clean install -Pdist -DskipTests=true
mv distribution/target/apache*.tar.gz ../builds
cd ../builds
rm -r druid
tar -xzf apache*.tar.gz
rm apache*.tar.gz
mv apache* druid
cd druid
export GOOGLE_APPLICATION_CREDENTIALS=/Users/manishgill/Workspace/Personal/druid/incubator-druid/.gcp_credentials.json
./bin/start-single-server-small
