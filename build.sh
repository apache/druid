#!/bin/bash -e

PROJECT=druid

DIST_DIR=dist/tar

SCRIPT_DIR=`dirname $0`
pushd $SCRIPT_DIR
SCRIPT_DIR=`pwd`
popd

VERSION=`cat pom.xml | grep version | head -4 | tail -1 | sed 's_.*<version>\([^<]*\)</version>.*_\1_'`

echo Using Version[${VERSION}]

mvn clean
mvn package

if [ $? -ne "0" ]; then
    echo "mvn package failed"
    exit 2;
fi

echo " "
echo "        The following self-contained jars (and more) have been built:"
echo " "
find . -name '*-selfcontained.jar'
echo " "
echo "For examples, see: "
echo " "
ls -1 examples/*/*sh
echo " "
echo "See also http://druid.io/docs/latest"
