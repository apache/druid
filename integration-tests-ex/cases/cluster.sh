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
# versions and other settings gathered when the test image was built.
# Maps category names to cluster names. The mapping here must match
# that in the test category classes when @Cluster is used.

# Enable for debugging
#set -x

export MODULE_DIR=$(cd $(dirname $0) && pwd)

USAGE="Usage: $0 category [-h|help|up|down|status|compose-cmd]"

# Cluster name is required
if [ $# -eq 0 ]; then
	echo "$USAGE" 1>&2
	exit 1
fi

# The untranslated category is used for the local name of the
# shared folder.
CATEGORY=$1
shift

# DRUID_INTEGRATION_TEST_GROUP is used in
# docker-compose files and here. Despite the name, it is the
# name of the cluster configuration we want to run, not the
# test category. Multiple categories an map to the same cluster
# definition.

# Map from category name to shared cluster definition name.
# Add an entry here if you create a new category that shares
# a definition.
case $CATEGORY in
	"InputFormat")
		export DRUID_INTEGRATION_TEST_GROUP=BatchIndex
		;;
	*)
		export DRUID_INTEGRATION_TEST_GROUP=$CATEGORY
		;;
esac

CLUSTER_DIR=$MODULE_DIR/cluster/$DRUID_INTEGRATION_TEST_GROUP
if [ ! -d $CLUSTER_DIR ]; then
	echo "Cluster directory $CLUSTER_DIR does not exist." 1>&2
	echo "$USAGE" 1>&2
	exit 1
fi

# 'up' command by default, else whatever is the argument
CMD='up'
if [ $# -ge 1 ]; then
	CMD=$1
fi

# All commands need env vars
ENV_FILE=$MODULE_DIR/../image/target/env.sh
if [ ! -f $ENV_FILE ]; then
	echo "Please build the Docker test image before testing" 1>&2
	exit 1
fi

source $ENV_FILE

export TARGET_DIR=$MODULE_DIR/target
export SHARED_DIR=$TARGET_DIR/$CATEGORY

# Used in docker-compose files
export OVERRIDE_ENV=${OVERRIDE_ENV:-}

# Print environment for debugging
#env

# Dump lots of information to debug Docker failures when run inside
# of a build environment where we can't inspect Docker directly.
function show_status {
	echo "===================================="
	ls -l target/shared
	echo "docker ps -a"
	docker ps -a
	# Was: --filter status=exited
	for id in $(docker ps -a --format "{{.ID}}"); do
	    echo "===================================="
	    echo "Logs for Container ID $id"
		docker logs $id | tail -n 20
	done
	echo "===================================="
}

case $CMD in
	"-h" | "-?")
		echo "$USAGE"
		;;
	"help")
		echo "$USAGE"
		docker-compose help
		;;
	"up")
		echo "Starting cluster $DRUID_INTEGRATION_TEST_GROUP"
		mkdir -p $SHARED_DIR
		# Must start with an empty DB to keep MySQL happy
		rm -rf $SHARED_DIR/db
		mkdir -p $SHARED_DIR/logs
		mkdir -p $SHARED_DIR/tasklogs
		mkdir -p $SHARED_DIR/db
		mkdir -p $SHARED_DIR/kafka
		mkdir -p $SHARED_DIR/resources
		cp $MODULE_DIR/assets/log4j2.xml $SHARED_DIR/resources
		# Permissions in some build setups are screwed up. See above. The user
		# which runs Docker does not have permission to write into the /shared
		# directory. Force ownership to allow writing.
		chmod -R a+rwx $SHARED_DIR
	    cd $CLUSTER_DIR
		docker-compose up -d
		# Enable the following for debugging
		#show_status
		;;
	"status")
	    cd $CLUSTER_DIR
		show_status
		;;
	"down")
		# Enable the following for debugging
		#show_status
	    cd $CLUSTER_DIR
		docker-compose $CMD
		;;
	"*")
	    cd $CLUSTER_DIR
		docker-compose $CMD
		;;
esac
