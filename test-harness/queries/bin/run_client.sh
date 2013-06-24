#!/bin/bash
ENV_CLUSTER="`dirname $0`/../../env-cluster.sh"
source $ENV_CLUSTER

set +u
shopt -s xpg_echo
shopt -s expand_aliases
trap "exit 1" 1 2 3 15

QUERY_FILE="${1:-group_by}_query.body"
[ ! -e $QUERY_FILE ]  &&  echo "expecting file $QUERY_FILE to be in current directory"  &&  exit 2

for delay in 1 15 15 15 15 15 15 15 15 15 15
 do
   echo "sleep for $delay seconds..."
   echo " " 
   sleep $delay
   time curl -sX POST "http://${DRUID_BROKER_HOST}:${DRUID_BROKER_PORT}/druid/v2/?pretty=true" -H 'content-type: application/json'  -d @$QUERY_FILE
done
echo "$0 finished"
