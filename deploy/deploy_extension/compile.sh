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
rm -rf distribution/target/bigo-extensions/*
mkdir -p distribution/target/bigo-extensions/druid-bigo-extensions
cp -f extensions-contrib/bigo-extensions/target/druid-bigo-extensions-0.14.2-incubating.jar \
    distribution/target/bigo-extensions/druid-bigo-extensions
cd distribution/target/bigo-extensions/druid-bigo-extensions

wget "http://central.maven.org/maven2/org/apache/hadoop/hadoop-common/2.8.5/hadoop-common-2.8.5.jar"
cd ../../

tar -czvf bigo-extensions.tar.gz bigo-extensions