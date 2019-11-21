#!/bin/bash
set -x

HADOOP_CLASSPATH=${HADOOP_CLASSPATH:-`$HADOOP_HOME/bin/hadoop classpath`}
DRUID_CLASSPATH=${DRUID_CLASSPATH:-"jars"}

# expand the classpath to a list of jars separated by :
DRUID_USER_CLASSPATH=`ls -1 $DRUID_CLASSPATH | xargs -I{} echo $DRUID_CLASSPATH/{} | tr '\n' ':'`

FS_FQDN=${FS_FQDN:-com.amazon.ws.emr.hadoop.fs.EmrFileSystem}
DRUID_JAVA_OPTS="-Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath conf/:$HADOOP_CLASSPATH:$DRUID_CLASSPATH/* -Ddruid.hadoop.user.classpath=$DRUID_USER_CLASSPATH -Djava.library.path=$HADOOP_HOME/lib/native/ -Dhadoop.fs.s3n.impl=${FS_FQDN} -Dhadoop.fs.s3.impl=${FS_FQDN}"

$JAVA_HOME/bin/java $DRUID_JAVA_OPTS org.apache.druid.cli.Main index hadoop --no-default-hadoop "$@"

# Check the return value of the command
RETVAL=$?
if [ "${RETVAL}" -ne 0 ]; then
    echo "********* EXECUTION ERROR ************"
    echo "The process completed with return code $RETVAL. See logs for more details."
    exit ${RETVAL}
fi

# Even though we might have gotten a return value of 0, it still could have failed.  Scan the last 250 lines of the log for valid INFO/WARN/ERROR messages.
ERROR_LINES=$(tail -n 250 stdout.log | grep -E -e '^([[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}[[:space:]][[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}).*' | awk '{print $3}' | grep 'ERROR' | wc -l)
if [ "${ERROR_LINES}" -ne 0 ]; then
    echo "********* EXECUTION FAILURE ************"
    echo "The process completed with a successful return code but errors were found in the logs. See logs for more details."
    RETVAL=1
fi

# Even though we might have gotten a return value of 0, it still could have failed.  Scan the last 250 lines of the log for valid INFO/WARN/ERROR messages.
ERROR_LINES=$(tail -n 250 stderr.log | grep -E -e '^([[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}[[:space:]][[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}).*' | awk '{print $3}' | grep 'ERROR' | wc -l)
if [ "${ERROR_LINES}" -ne 0 ]; then
    echo "********* EXECUTION FAILURE ************"
    echo "The process completed with a successful return code but errors were found in the logs. See logs for more details."
    RETVAL=1
fi

exit ${RETVAL}
