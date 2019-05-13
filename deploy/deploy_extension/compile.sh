#!/usr/bin/env bash
echo "compiling project druid 14"
cd ../../
mvn clean install -Papache-release,dist,rat,bundle-contrib-exts \
-DskipTests -Drat.skip=true -Dgpg.skip=true  -Dcheckstyle.skip=true
mkdir -p distribution/target/bigo-extensions
mkdir -p distribution/target/bigo-extensions/druid-bigo-extensions

cp -f extensions-contrib/bigo-extensions/target/druid-bigo-extensions-0.14.1-incubating.jar \
    distribution/target/bigo-extensions/druid-bigo-extensions

cp -f distribution/target/hadoop-dependency/hadoop-common-2.7.3.jar \
    distribution/target/bigo-extensions/druid-bigo-extensions

cd distribution/target/
tar -czvf bigo-extensions.tar.gz bigo-extensions