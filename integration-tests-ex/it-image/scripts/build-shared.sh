#!/usr/bin/env bash

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

# This script assumes that the working directory is
# $DRUID_DEV/druid-integration-tests

SHARED_DIR=target/shared
mkdir -p $SHARED_DIR/hadoop_xml
mkdir -p $SHARED_DIR/hadoop-dependencies
mkdir -p $SHARED_DIR/logs
mkdir -p $SHARED_DIR/tasklogs
mkdir -p $SHARED_DIR/docker/extensions
mkdir -p $SHARED_DIR/docker/credentials

DRUID_DEV=../..

# Setup client keystore
./docker/tls/generate-client-certs-and-keystores.sh

# One of the integration tests needs the wikiticker sample data

DATA_DIR=$SHARED_DIR/wikiticker-it
mkdir -p $DATA_DIR
cp $DRUID_DEV/examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json.gz $DATA_DIR/wikiticker-2015-09-12-sampled.json.gz
cp test-data/wiki-simple-lookup.json $DATA_DIR/wiki-simple-lookup.json
cp test-data/wikipedia.desc $DATA_DIR/wikipedia.desc
