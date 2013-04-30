#!/usr/bin/env bash
echo "This will run RealtimeStandaloneMain service in background"
set +u
shopt -s xpg_echo
shopt -s expand_aliases

PF=./twitter4j.properties

# if twitter4j.properties already exists, then user is okay with having twitter pw in file.
#  Otherwise a twitter4j.properties file in curr. dir. is made temporarily for twitter login.
if [ -e "$PF" ]; then
    PF_CLEANUP="date"
    trap "exit 1" 1 2 3 15
else
    PF_CLEANUP="/bin/rm $PF"
    trap "${PF_CLEANUP} ; exit 1" 1 2 3 15
    touch $PF
    chmod 700 $PF
    echo "   Your twitter user login name and pw is needed "
    read -p 'twitter username? '  TWIT_USER
    read -s -p 'twitter password? ' TWIT_PW
    echo "user=${TWIT_USER}" >> $PF
    echo "password=${TWIT_PW}" >> $PF
    TWIT_PW=""
fi
trap "${PF_CLEANUP} ; exit 1" 1 2 3 15

# props are set in src/main/resources/runtime.properties

[ -d /tmp/twitter_realtime ]  &&  echo "cleaning up from previous run.."  &&  /bin/rm -fr /tmp/twitter_realtime

# check spec file exists
[ ! -e twitter_realtime.spec ]  &&  echo "expecting file twitter_realtime.spec (as specified by property druid.realtime.specFile) to be in current directory"  &&  exit 3

OPT_PROPS=""
#OPT_PROPS="-Dtwitter4j.debug=true -Dtwitter4j.http.prettyDebug=true"

#  start RealtimeNode process
#
echo "Log output of service can be found in ./RealtimeNode.out"
java -Xmx600m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath target/druid-examples-twitter-*-selfcontained.jar $OPT_PROPS -Dtwitter4j.http.useSSL=true druid.examples.RealtimeStandaloneMain  >RealtimeNode.out 2>&1  &
PID=$!

trap "${PF_CLEANUP} ; kill ${PID} ; exit 1" 1 2 3 15

sleep 4
grep com.metamx.druid.realtime.TwitterSpritzerFirehoseFactory RealtimeNode.out | awk '{ print $7,$8,$9,$10,$11,$12,$13,$14,$15 }'
sleep 17
grep 'twitter4j.TwitterStreamImpl' RealtimeNode.out > p$$
grep 'Waiting for [0-9]000' p$$
RC1=$?
grep '401:Authentication credentials' p$$
RC2=$?
/bin/rm p$$
if [ $RC1 -eq 0  -a  $RC2 -eq 0 ]; then
    # avoid getting your ip addr blocked via too many failed login attempts
    kill $PID
    ${PF_CLEANUP}
    echo "twitter login failed, RealtimeStandaloneMain process terminated"
    exit 7
fi

wait $PID
# clean up twitter credentials after wait since possible reconnection will need it
${PF_CLEANUP}
echo "RealtimeStandaloneMain finished"

