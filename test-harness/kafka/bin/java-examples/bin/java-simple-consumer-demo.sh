#!/bin/bash

base_dir=$(dirname $0)/../../..

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

echo $CLASSPATH

if [ -z "$KAFKA_PERF_OPTS" ]; then
  KAFKA_OPTS="-Xmx512M -server -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=3333 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $KAFKA_OPTS -cp $CLASSPATH kafka.examples.SimpleConsumerDemo $@

