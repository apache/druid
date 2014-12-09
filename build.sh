#!/bin/bash -eu

BASE=$(cd $(dirname $0) && pwd)

VERSION=`cat pom.xml | grep '<version>' | head -1 | tail -1 | sed 's_.*<version>\([^<]*\)</version>.*_\1_'`

echo "Building Version [${VERSION}]"

mvn -U -B clean package

JARS=$(find "$BASE" -name "*-$VERSION-selfcontained.jar" | sed -e 's/^/  /')

cat <<EOF

The following self-contained jars (and more) have been built:
$JARS
EOF
