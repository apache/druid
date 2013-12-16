#! /bin/bash -e
SCRIPT_DIR=$(cd $(dirname "$0") && pwd)

if [ -z ${1} ]; then 
    VERSION=$(cat $SCRIPT_DIR/../../pom.xml | grep version | head -4 | tail -1 | sed 's_.*<version>\([^<]*\)</version>.*_\1_')
else
    VERSION=${1}
fi

#if [ -z "$(git tag -l "druid-$VERSION")" ]
if [ -z "$(git tag -l "druid-$VERSION")" ] && [ "$VERSION" != "latest" ]; then
  echo "Version tag does not exist: druid-$VERSION"
  exit 1;
fi

WORKING_DIR=$(mktemp -d -t druid-docs-deploy)

echo Using Version [${VERSION}]
echo Script in [${SCRIPT_DIR}]
echo Deploying to [${WORKING_DIR}]

if [ -d ${WORKING_DIR} ]; then
    echo DELETING ${WORKING_DIR}
    rm -rf ${WORKING_DIR}
fi

git clone git@github.com:druid-io/druid-io.github.io.git ${WORKING_DIR}

DOC_DIR=${WORKING_DIR}/docs/${VERSION}/

mkdir -p ${DOC_DIR}
cp -r ${SCRIPT_DIR}/../content/* ${DOC_DIR}

BRANCH=docs-${VERSION}

pushd ${WORKING_DIR}
git checkout -b ${BRANCH}
git add .
git commit -m "Deploy ${VERSION} docs"
git push origin ${BRANCH}
popd

rm -rf ${WORKING_DIR}
