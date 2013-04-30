#!/usr/bin/env bash
echo "  commandline query to RealtimeStandaloneMain service using curl"
echo "    before running this, do:  run_server.sh"

set +u
shopt -s xpg_echo
shopt -s expand_aliases
trap "exit 1" 1 2 3 15

[ ! -e query.body ]  &&  echo "expecting file query.body to be in current directory"  &&  exit 2

for delay in 5 30 30 30 30 30 30 30 30 30 30 
 do
   echo "sleep for $delay seconds..."
   echo " " 
   sleep $delay
   curl -X POST 'http://localhost:8080/druid/v2/?w' -H 'content-type: application/json'  -d @query.body
   echo " "
   echo " "
done

echo "$0 finished; you might want to terminate the background process run_server.sh"
