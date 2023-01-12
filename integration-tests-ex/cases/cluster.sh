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

function usage {
	cat <<EOF
Usage: $0 cmd [category]
  -h, help
      Display this message
  up category
      Start the cluster
  down category
      Stop the cluster
  status category
      Status of the cluster (for debugging within build scripts)
  compose-cmd category
      Pass the command to Docker compose.
EOF
}

# Command name is required
if [ $# -eq 0 ]; then
	usage 1>&2
	exit 1
fi

CMD=$1
shift

# All commands need env vars
ENV_FILE=$MODULE_DIR/../image/target/env.sh
if [ ! -f $ENV_FILE ]; then
	echo "Please build the Docker test image before testing" 1>&2
	exit 1
fi

source $ENV_FILE

function category {
	if [ $# -eq 0 ]; then
		usage 1>&2
		exit 1
	fi
	export CATEGORY=$1
	# The untranslated category is used for the local name of the
	# shared folder.

	# DRUID_INTEGRATION_TEST_GROUP is used in
	# docker-compose files and here. Despite the name, it is the
	# name of the cluster configuration we want to run, not the
	# test category. Multiple categories can map to the same cluster
	# definition.

	# Map from category name to shared cluster definition name.
	# Add an entry here if you create a new category that shares
	# a definition.
	case $CATEGORY in
		"InputSource")
			export DRUID_INTEGRATION_TEST_GROUP=BatchIndex
			;;
		"InputFormat")
			export DRUID_INTEGRATION_TEST_GROUP=BatchIndex
			;;
		"Catalog")
			export DRUID_INTEGRATION_TEST_GROUP=BatchIndex
			;;
		*)
			export DRUID_INTEGRATION_TEST_GROUP=$CATEGORY
			;;
	esac

	export CLUSTER_DIR=$MODULE_DIR/cluster/$DRUID_INTEGRATION_TEST_GROUP
	if [ ! -d $CLUSTER_DIR ]; then
		echo "Cluster directory $CLUSTER_DIR does not exist." 1>&2
		echo "$USAGE" 1>&2
		exit 1
	fi

	export TARGET_DIR=$MODULE_DIR/target
	export SHARED_DIR=$TARGET_DIR/$CATEGORY
	export ENV_FILE="$TARGET_DIR/${CATEGORY}.env"
}

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

function build_shared_dir {
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
}

# Either generate the docker-compose file, or use "static" versions.
function docker_file {

	# If a template exists, generate the docker-compose.yaml file. Copy over the Common
	# folder.
	if [ -f "$CLUSTER_DIR/docker-compose.sh" ]; then
		export COMPOSE_DIR=$TARGET_DIR/cluster/$CATEGORY
		$CLUSTER_DIR/docker-compose.sh
		cp -r $MODULE_DIR/cluster/Common $TARGET_DIR/cluster
	else
		# Else, use the existing non-template file in place.
		export COMPOSE_DIR=$CLUSTER_DIR
		choose_static_file
	fi
}

# Each test that uses static (non-generated) docker-compose files
# must have a default docker-compose.yaml file which corresponds to using
# the MiddleManager (or no indexer). A test can optionally include a second file called
# docker-compose-indexer.yaml which uses the Indexer in place of Middle Manager.
function choose_static_file {
	export DOCKER_ARGS=""
	if [ -n "$DRUID_INTEGRATION_TEST_INDEXER" ]; then
	    # Sanity check: DRUID_INTEGRATION_TEST_INDEXER must be "indexer" or "middleManager"
	    # if it is set at all.
		if [ "$DRUID_INTEGRATION_TEST_INDEXER" != "indexer" ] && [ "$DRUID_INTEGRATION_TEST_INDEXER" != "middleManager" ]
		then
		  echo "DRUID_INTEGRATION_TEST_INDEXER must be 'indexer' or 'middleManager' (is '$DRUID_INTEGRATION_TEST_INDEXER')" 1>&2
		  exit 1
		fi
		if [ "$DRUID_INTEGRATION_TEST_INDEXER" == "indexer" ]; then
			compose_file=docker-compose-indexer.yaml
			if [ ! -f "$CLUSTER_DIR/$compose_file" ]; then
			  echo "DRUID_INTEGRATION_TEST_INDEXER=$DRUID_INTEGRATION_TEST_INDEXER, but $CLUSTER_DIR/$compose_file is missing" 1>&2
			  exit 1
		    fi
		   export DOCKER_ARGS="-f $compose_file"
		fi
	fi
}

# Print environment for debugging
#env

case $CMD in
	"-h" )
		usage
		;;
	"help" )
		usage
		docker-compose help
		;;
	"up" )
		category $*
		echo "Starting cluster $DRUID_INTEGRATION_TEST_GROUP"
		build_shared_dir
		docker_file
	    cd $COMPOSE_DIR
		docker-compose $DOCKER_ARGS up -d
		# Enable the following for debugging
		#show_status
		;;
	"status" )
		category $*
		docker_file
	    cd $COMPOSE_DIR
		show_status
		;;
	"down" )
		category $*
		# Enable the following for debugging
		#show_status
		docker_file
	    cd $COMPOSE_DIR
		docker-compose $DOCKER_ARGS $CMD
		;;
	"*" )
		category $*
		docker_file
	    cd $COMPOSE_DIR
		docker-compose $DOCKER_ARGS $CMD
		;;
esac
