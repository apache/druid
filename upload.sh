#!/bin/bash -e

#
# Script to upload tarball of assembly build to static.druid.io for serving
#

if [ $# -lt 1 ]; then
  echo "Usage: $0 <version>" >&2
  exit 2
fi

VERSION=$1
DRUID_TAR=druid-$VERSION-bin.tar.gz
MYSQL_TAR=mysql-metadata-storage-$VERSION.tar.gz
S3PATH=s3://static.druid.io/artifacts/releases

if [ ! -z "`s3cmd ls "$S3PATH/$DRUID_TAR"`" ]; then
  echo "ERROR: Refusing to overwrite $S3PATH/$DRUID_TAR" >&2
  exit 2
fi

if [ ! -z "`s3cmd ls "$S3PATH/$MYSQL_TAR"`" ]; then
  echo "ERROR: Refusing to overwrite $S3PATH/$MYSQL_TAR" >&2
  exit 2
fi

s3cmd put distribution/target/$DRUID_TAR $S3PATH/$DRUID_TAR
s3cmd put distribution/target/$MYSQL_TAR $S3PATH/$MYSQL_TAR
