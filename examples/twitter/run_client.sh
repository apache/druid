#!/usr/bin/env bash
echo "  commandline query to RealtimeStandaloneMain service using curl"
echo "    before running this, do:  run_server.sh"

set +u
shopt -s xpg_echo
shopt -s expand_aliases
trap "exit 1" 1 2 3 15

QUERY_FILE="${1:-group_by}_query.body"

[ ! -e $QUERY_FILE ]  &&  echo "expecting file $QUERY_FILE to be in current directory"  &&  exit 2

for delay in 5 15 15 15 15 15 15 15 15 15 15
 do
   echo "sleep for $delay seconds..."
   echo " " 
   sleep $delay
   curl -X POST 'http://localhost:8080/druid/v2/' -H 'content-type: application/json'  -d @$QUERY_FILE
   echo " "
   echo " "
done

echo "$0 finished; you might want to terminate the background process run_server.sh"
