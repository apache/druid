#! /bin/bash -e

SCRIPT_DIR=`dirname $0`
pushd $SCRIPT_DIR
SCRIPT_DIR=`pwd`
popd

if [ -z ${1} ]; then 
    pushd $SCRIPT_DIR
    VERSION=`cat ../../pom.xml | grep version | head -4 | tail -1 | sed 's_.*<version>\([^<]*\)</version>.*_\1_'`
    popd
else
    VERSION=${1}
fi

WORKING_DIR=/tmp/docs-deploy

echo Using Version[${VERSION}]
echo Script in [${SCRIPT_DIR}]
echo Deploying to [${WORKING_DIR}]

if [ -d ${WORKING_DIR} ]; then
    echo DELETING ${WORKING_DIR}
    rm -rf ${WORKING_DIR}
fi

git clone git@github.com:druid-io/druid-io.github.io.git ${WORKING_DIR}

DOC_DIR=${WORKING_DIR}/docs/${VERSION}/

cp ${SCRIPT_DIR}/../_layouts/doc* ${WORKING_DIR}/_layouts/
mkdir -p ${DOC_DIR}
cp -r ${SCRIPT_DIR}/../content/* ${DOC_DIR}

BRANCH=docs-${VERSION}

pushd ${WORKING_DIR}
git checkout -b ${BRANCH}
git add .
git commit -m "Deploy new docs version ${VERSION}"
git push origin ${BRANCH}
popd

rm -rf ${WORKING_DIR}



