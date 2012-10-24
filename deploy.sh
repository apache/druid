#!/bin/bash -e

PROJECT=druid

DIST_DIR=dist/tar

SCRIPT_DIR=`dirname $0`
pushd $SCRIPT_DIR
SCRIPT_DIR=`pwd`
popd

VERSION=`cat pom.xml | grep version | head -2 | tail -1 | sed 's_.*<version>\([^<]*\)</version>.*_\1_'`
TAR_FILE=${SCRIPT_DIR}/${PROJECT}-${VERSION}.tar.gz

${SCRIPT_DIR}/build.sh
if [ $? -ne "0" ]; then
    echo "Build failed"
    exit 2
fi

echo Deploying ${TAR_FILE}
s3cmd put ${TAR_FILE} s3://metamx-galaxy-bin/binaries/
