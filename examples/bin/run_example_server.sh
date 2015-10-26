#!/usr/bin/env bash
echo "This will run a stand-alone version of Druid"
set +u
shopt -s xpg_echo
shopt -s expand_aliases
trap "exit 1" 1 2 3 15

SCRIPT_DIR=`dirname $0`
MAVEN_DIR="${SCRIPT_DIR}/extensions-repo"

if [[ ! -d "${SCRIPT_DIR}/lib" || ! -d "${SCRIPT_DIR}/config" ]]; then
  echo "This script appears to be running from the source location. It must be run from its deployed location."
  echo "After building, unpack services/target/druid-services-*-SNAPSHOT-bin.tar.gz, and run the script unpacked there."
  exit 2
fi

CURR_DIR=`pwd`
cd ${SCRIPT_DIR}
SCRIPT_DIR=`pwd`
cd ${CURR_DIR}

[ -d /tmp/example ]  &&  echo "Cleaning up from previous run.."  &&  /bin/rm -fr /tmp/example

source $SCRIPT_DIR/select_example.sh

select_example SPEC_FILE "${SCRIPT_DIR}/examples" "*_realtime.spec" "${1}" "${1}_realtime.spec"

EXAMPLE_LOC=$(dirname $SPEC_FILE)
# run before script if it exists
if [ -x ${EXAMPLE_LOC}/before.sh ]; then
    trap "set +x; cd ${EXAMPLE_LOC} && ./after.sh && cd ${CURR_DIR}; exit 1" EXIT
    cd ${EXAMPLE_LOC}
    ./before.sh
    cd ${CURR_DIR}
fi

#  start process
JAVA_ARGS="-Xmx512m -Duser.timezone=UTC -Dfile.encoding=UTF-8"
JAVA_ARGS="${JAVA_ARGS} -Ddruid.realtime.specFile=${SPEC_FILE}"
JAVA_ARGS="${JAVA_ARGS} -Ddruid.publish.type=noop"

DRUID_CP=${EXAMPLE_LOC}
#For a pull
DRUID_CP=${SCRIPT_DIR}/../config/realtime:${DRUID_CP}
#For the kit
DRUID_CP=${DRUID_CP}:${SCRIPT_DIR}/config/_common
DRUID_CP=${DRUID_CP}:${SCRIPT_DIR}/config/realtime
DRUID_CP=${DRUID_CP}:${SCRIPT_DIR}/lib/*

echo "Running command:"

(set -x; java ${JAVA_ARGS} -classpath ${DRUID_CP}  io.druid.cli.Main example realtime)
