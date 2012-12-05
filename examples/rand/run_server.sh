#!/usr/bin/env bash
echo "This will run RealtimeStandaloneMain service in background"
set +u
shopt -s xpg_echo
shopt -s expand_aliases
trap "exit 1" 1 2 3 15

# props are set in src/main/resources/runtime.properties

[ -d /tmp/rand_realtime ]  &&  echo "cleaning up from previous run.."  &&  /bin/rm -fr /tmp/rand_realtime

# check spec file exists
[ ! -e rand_realtime.spec ]  &&  echo "expecting file rand_realtime.spec (as specified by property druid.realtime.specFile) to be in current directory"  &&  exit 3

#  start RealtimeNode process
#
echo "Log output of service can be found in ./RealtimeNode.out"
java -Xmx600m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath target/druid-examples-rand-*-selfcontained.jar  druid.examples.RealtimeStandaloneMain  >RealtimeNode.out 2>&1  &
PID=$!

trap "kill $PID ; exit 1" 1 2 3 15
sleep 4
grep druid.examples.RandomFirehoseFactory RealtimeNode.out | awk '{ print $7,$8,$9,$10,$11,$12,$13,$14,$15 }'
wait $PID
echo "RealtimeStandaloneMain finished"

