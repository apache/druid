#!/usr/bin/env bash
echo "This will run a query against a stand-alone version of Druid"
echo "    before running this, do:  run_example_server.sh"
set +u
shopt -s xpg_echo
shopt -s expand_aliases
trap "exit 1" 1 2 3 15

SCRIPT_DIR=`dirname $0`
CURR_DIR=`pwd`
cd ${SCRIPT_DIR}
SCRIPT_DIR=`pwd`
cd ${CURR_DIR}

source $SCRIPT_DIR/select_example.sh

select_example QUERY_FILE "${SCRIPT_DIR}/examples" "*query.body" "${1}" "query.body"

cat ${QUERY_FILE}
for delay in 5 30 30 30 30 30 30 30 30 30 30
 do
   echo "sleep for $delay seconds..."
   echo " "
   sleep $delay
   curl -X POST 'http://localhost:8084/druid/v2/?pretty' -H 'content-type: application/json'  -d "`cat ${QUERY_FILE}`"
   echo " "
   echo " "
done

echo "$0 finished"
