#!/usr/bin/env bash
echo "This will run a stand-alone version of Druid"
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

[ -d /tmp/example ]  &&  echo "Cleaning up from previous run.."  &&  /bin/rm -fr /tmp/example

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

SPEC_FILE=${EXAMPLE_LOC}/${EXAMPLE}_realtime.spec

# check spec file exists
[ ! -e ${SPEC_FILE} ]  &&  echo "Expecting file ${SPEC_FILE} to exist, it didn't"  &&  exit 3

# run before script if it exists
if [ -e ${EXAMPLE_LOC}/before.sh ]; then
    trap "set +x; cd ${EXAMPLE_LOC} && ./after.sh && cd ${CURR_DIR}; exit 1" EXIT
    cd ${EXAMPLE_LOC}
    ./before.sh
    cd ${CURR_DIR}
fi

#  start process
JAVA_ARGS="-Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8"
JAVA_ARGS="${JAVA_ARGS} -Ddruid.realtime.specFile=${SPEC_FILE}"


DRUID_CP=${EXAMPLE_LOC}
#For a pull
DRUID_CP=${DRUID_CP}:${SCRIPT_DIR}/../config/realtime
#For the kit
DRUID_CP=${DRUID_CP}:${SCRIPT_DIR}/lib/*
DRUID_CP=${DRUID_CP}:${SCRIPT_DIR}/config/realtime

echo "Running command:"

(set -x; java ${JAVA_ARGS} -classpath ${DRUID_CP}  io.druid.cli.Main example realtime)
