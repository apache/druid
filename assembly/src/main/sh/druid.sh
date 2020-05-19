#!/bin/bash
set -x

HADOOP_CLASSPATH=${HADOOP_CLASSPATH:-`$HADOOP_HOME/bin/hadoop classpath`}
DRUID_CLASSPATH=${DRUID_CLASSPATH:-"jars"}

# expand the classpath to a list of jars separated by :
DRUID_USER_CLASSPATH=`ls -1 $DRUID_CLASSPATH | xargs -I{} echo $DRUID_CLASSPATH/{} | tr '\n' ':'`

FS_FQDN=${FS_FQDN:-com.amazon.ws.emr.hadoop.fs.EmrFileSystem}
DRUID_JAVA_OPTS="-Xmx2560m -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager  -Dlog4j.configurationFile=${GENIE_JOB_DIR}/log4j2.xml -Djava.io.tmpdir=/mnt/tmp/druid/ -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath conf/:$DRUID_CLASSPATH/*:$HADOOP_CLASSPATH -Ddruid.hadoop.user.classpath=$DRUID_USER_CLASSPATH -Djava.library.path=$HADOOP_HOME/lib/native/ -Dhadoop.fs.s3n.impl=${FS_FQDN} -Dhadoop.fs.s3.impl=${FS_FQDN} -Dhadoop.tmp.dir=${GENIE_JOB_DIR}/tmp"

$JAVA_HOME/bin/java $DRUID_JAVA_OPTS org.apache.druid.cli.Main index hadoop --no-default-hadoop "$@"

# Check the return value of the command
RETVAL=$?
if [ "${RETVAL}" -ne 0 ]; then
    echo "********* EXECUTION ERROR ************"
    echo "The process completed with return code $RETVAL. See logs for more details."
fi

exit ${RETVAL}
