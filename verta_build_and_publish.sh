#!/bin/bash

set -eo pipefail

export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
export VERSION_SUFFIX="$(git rev-parse --abbrev-ref HEAD)"
export VERSION_SUFFIX=$(echo $VERSION_SUFFIX | sed 's,/,-,g' | tr '[:upper:]' '[:lower:]')
export PROJECT_VERSION=$(mvn help:evaluate -Dexpression=revision -q -DforceStdout)
export PROJECT_VERSION=$(echo $PROJECT_VERSION | sed "s,-SNAPSHOT,-${VERSION_SUFFIX}-SNAPSHOT,g")
export MAVEN_PARAMS='-Pdist-hadoop3,hadoop3,bundle-contrib-exts -Dpmd.skip=true -Denforcer.skip -Dforbiddenapis.skip=true -Dcheckstyle.skip=true -Danimal.sniffer.skip=true -Djacoco.skip=true -DskipTests'
mvn -B versions:set -DnewVersion=$PROJECT_VERSION > /dev/null
mvn -B deploy $MAVEN_PARAMS || {
    mvn -B versions:revert > /dev/null
    # strange behavior where website needs separate call to revert
    `cd website && mvn -B versions:revert > /dev/null`
    exit 1
}
mvn -B versions:revert > /dev/null
# strange behavior where website needs separate call to revert
`cd website && mvn -B versions:revert > /dev/null`