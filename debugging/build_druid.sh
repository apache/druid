#!/bin/bash
set -e
cd ../

if [ "$1" != "-pi" ]; then
  mvn clean install -Pdist -DskipTests=true
else
  mvn clean install -DskipTests=true -Dcheckstyle.skip -Dpmd.skip -pl :druid-pubsub-indexing-service
  mvn clean install -Pdist -DskipTests=true -Dcheckstyle.skip -Dpmd.skip -pl :distribution
fi

mv distribution/target/apache*.tar.gz ../builds
cd ../builds
rm -r druid
tar -xzf apache*.tar.gz
rm apache*.tar.gz
mv apache* druid

