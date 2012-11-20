#!/usr/bin/env bash
# Script to run util DruidSetup which will initialize zookeeper locations, properties, and metadata store (MySQL or similar).
# The dump cmd of DruidSetup will dump properties stored at and zpaths of zookeeper.
# Run with no args to get usage.

which java >/dev/null
WJ=$?
if [ "${JAVA_HOME}" ]; then
  RUN_JAVA=$JAVA_HOME/bin/java
elif [ $WJ -eq 0 ]; then
  RUN_JAVA=java
fi

[ -z "${RUN_JAVA}" ]  &&  echo "env var JAVA_HOME is not defined and java not in path"  &&  exit 1

DRUID_DIR=$(cd $(dirname $0)/.. ; pwd)

DRUID_JAR="$(ls -1 $(find $DRUID_DIR -name 'druid-services*selfcontained.jar') |head -1)"
[ -z "${DRUID_JAR}" ]  &&  echo "unable to find druid server jar"  &&  exit 2
echo "using ${DRUID_JAR}"
echo 

$RUN_JAVA -cp "${DRUID_JAR}" -Dlog4j.configuration=file://${DRUID_DIR}/install/log4j.xml -Duser.timezone=UTC -Dfile.encoding=UTF-8  com.metamx.druid.utils.DruidSetup $*

[ -e ${DRUID_DIR}/install/druid_setup.log ]  &&  egrep "WARN|ERROR|FATAL" ${DRUID_DIR}/install/druid_setup.log
