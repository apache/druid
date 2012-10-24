#!/bin/bash -e

PROJECT=druid

DIST_DIR=dist/tar

SCRIPT_DIR=`dirname $0`
pushd $SCRIPT_DIR
SCRIPT_DIR=`pwd`
popd

VERSION=`cat pom.xml | grep version | head -2 | tail -1 | sed 's_.*<version>\([^<]*\)</version>.*_\1_'`
TAR_FILE=${SCRIPT_DIR}/${PROJECT}-${VERSION}.tar.gz

echo Using Version[${VERSION}] and creating zip file ${TAR_FILE}

rm -f ${TAR_FILE}
mvn clean
mvn package

if [ $? -ne "0" ]; then
    echo "mvn package failed"
    exit 2;
fi

rm -rf ${DIST_DIR}
mkdir -p ${DIST_DIR}/lib

cp binary-artifact/target/${PROJECT}-binary-artifact-${VERSION}-selfcontained.jar ${DIST_DIR}/lib/
cp -r bin ${DIST_DIR}/ # (bin/ is provided by java-shell)

cd ${DIST_DIR}
tar czf ${TAR_FILE} *
echo
echo Created ${TAR_FILE}:
tar tf ${TAR_FILE} | sed -r 's/^/  /'
