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

EXAMPLES_DIR=${SCRIPT_DIR}/examples

EXAMPLE=$1
if [ -z ${EXAMPLE} ] ; then
    echo "Please specify an example type."
    echo "Examples availables:"
    echo `ls ${EXAMPLES_DIR} | grep -v indexing`
    read -p "> " EXAMPLE
    echo " "
fi

EXAMPLE_LOC=${EXAMPLES_DIR}/${EXAMPLE}

while [[ ! -e ${EXAMPLE_LOC} ]] ; do
    echo "Unknown example ${EXAMPLE}, please specify a known example."
    echo "Known examples:"
    echo `ls ${EXAMPLES_DIR}`
    read -p "> " EXAMPLE
    EXAMPLE_LOC=${EXAMPLES_DIR}/${EXAMPLE}
    echo " "
done

QUERY_FILE=${EXAMPLE_LOC}/query.body

[ ! -e ${QUERY_FILE} ]  &&  echo "expecting file ${QUERY_FILE} to be in current directory"  &&  exit 2

echo "Running ${EXAMPLE} query:"
cat ${QUERY_FILE}
for delay in 5 30 30 30 30 30 30 30 30 30 30 
 do
   echo "sleep for $delay seconds..."
   echo " " 
   sleep $delay
   curl -X POST 'http://localhost:8083/druid/v2/?w' -H 'content-type: application/json'  -d "`cat ${QUERY_FILE}`"
   echo " "
   echo " "
done

echo "$0 finished"
