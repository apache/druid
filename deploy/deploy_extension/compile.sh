#!/usr/bin/env bash
echo "compiling project druid 14"

# packaging extensions-contrib/bigo-extensions
cd ../../extensions-contrib/bigo-extensions
mvn package \
-DskipTests \
-Dcheckstyle.skip=true \
-Dmaven.javadoc.skip=true
cd ../../
mkdir -p distribution/target/bigo-extensions
mkdir -p distribution/target/bigo-extensions/druid-bigo-extensions
cp -f extensions-contrib/bigo-extensions/target/druid-bigo-extensions-0.14.1-incubating.jar \
    distribution/target/bigo-extensions/druid-bigo-extensions
cp -f distribution/target/hadoop-dependencies/hadoop-client/2.7.3/hadoop-common-2.7.3.jar \
    distribution/target/bigo-extensions/druid-bigo-extensions

#
cd distribution/target/
tar -czvf bigo-extensions.tar.gz bigo-extensions