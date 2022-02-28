#! /bin/bash

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
#--------------------------------------------------------------------

# Starts the test-specific test cluster using Docker compose using
# versions and other settings gathered when building the images.

SCRIPT_DIR=$(cd $(dirname $0) && pwd)

# 'up' command by default, else whatever is the argument
CMD='up'
if [ $# -ge 1 ]; then
	CMD=$1
fi

# All commands need env vars.

ENV_FILE=$SCRIPT_DIR/../it-image/target/env.sh
if [ ! -f $ENV_FILE ]; then
	echo "Please build the Docker test image before testing" 1>&2
	exit 1
fi

source $ENV_FILE

# Print environment for debugging
#env

export SHARED_DIR=$SCRIPT_DIR/target/shared

# Dummies just to get the compose files to shut up
# TODO: Remove after conversion
export OVERRIDE_ENV=
export DRUID_INTEGRATION_TEST_GROUP=batch-index

cd $SCRIPT_DIR/druid-cluster
if [ "$CMD" == 'up' ]; then
	# Must start with an empty DB to keep MySQL happy
	rm -rf $SHARED_DIR/db
	mkdir -p $SHARED_DIR/logs
	mkdir -p $SHARED_DIR/tasklogs
	mkdir -p $SHARED_DIR/db
	mkdir -p $SHARED_DIR/kafka
	mkdir -p $SHARED_DIR/resources
	cp $SCRIPT_DIR/../assets/log4j2.xml $SHARED_DIR/resources
	docker-compose up -d
else
	docker-compose $CMD
fi
