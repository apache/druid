#!/bin/bash -e
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

# Use s3cmd if available, otherwise try awscli
if command -v s3cmd >/dev/null 2>&1
then
  s3ls="s3cmd ls"
  s3cp="s3cmd put"
else
  s3ls="aws s3 ls"
  s3cp="aws s3 cp"
fi

if [ ! -z "`$s3ls "$S3PATH/$DRUID_TAR"`" ]; then
  echo "ERROR: Refusing to overwrite $S3PATH/$DRUID_TAR" >&2
  exit 2
fi

if [ ! -z "`$s3ls "$S3PATH/$MYSQL_TAR"`" ]; then
  echo "ERROR: Refusing to overwrite $S3PATH/$MYSQL_TAR" >&2
  exit 2
fi

$s3cp distribution/target/$DRUID_TAR $S3PATH/$DRUID_TAR
$s3cp distribution/target/$MYSQL_TAR $S3PATH/$MYSQL_TAR
