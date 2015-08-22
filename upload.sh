#!/bin/bash -e

#
# Script to upload tarball of assembly build to static.druid.io for serving
#

if [ $# -lt 1 ]; then
  echo "Usage: $0 <version>" >&2
  exit 2
fi

VERSION=$1
TAR=druid-$VERSION-bin.tar.gz
S3PATH=s3://static.druid.io/artifacts/releases

if [ ! -z "`s3cmd ls "$S3PATH/$TAR"`" ]; then
  echo "ERROR: Refusing to overwrite $S3PATH/$TAR" >&2
  exit 2
fi

s3cmd put distribution/target/$TAR $S3PATH/$TAR
