#!/usr/bin/env bash
echo "compiling project druid 14"
cd ../../
mvn clean install -Papache-release,dist,rat,bundle-contrib-exts \
-DskipTests -Drat.skip=true -Dgpg.skip=true  -Dcheckstyle.skip=true