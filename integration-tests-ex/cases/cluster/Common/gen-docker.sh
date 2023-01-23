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

# Generates docker-compose.yaml files for a test. Avoids the need for
# copy/paste to create test configs. Also automatically switches from
# MiddleManager to Indexer based on the setting of the
# USE_INDEXER env var.
#
# This script acts like a simple template engine. Since the generated
# files are simple, using shell commands for the template is not quite
# as crazy as it sounds. If needes get more complex, we can switch to
# the Freemarker template engine used to generate the Calcite parser.
#
# Create a test-specific file by creating a docker-compose.sh script in
# cluster/YourTest. It should look like this to start:
#
# set -e
#
# TEMPLATE=$0
# export MODULE_DIR=$(cd $(dirname $0) && pwd)
# CATEGORY=$(basename $MODULE_DIR)
#
# . $MODULE_DIR/../Common/gen-docker.sh
#
# gen_compose_file $CATEGORY
#
# This will get you a "generic" Druid cluster with one of each service,
# using the standard configs. The generated file will use a Middle Manager
# or Indexer based on the setting of USE_INDEXER.
#
# Next, determine how you want to customize your cluster. You do this by
# adding functions at the end of the above file (after the gen_compose_file
# line. Basically, you override (actually, replace) functions in the base
# file with functions unique to your setup.
#
# The functions cover each area of the docker-compose file: env files,
# env, volumes, etc. If you want to change just one service, create a
# gen_<service>_<item> function. If you want to change something for all
# service, create a gen_common_<item> function.
#
# This is easier to understand by looking at examples of existing files.
# Note that the per-service functions call the common functions. Note also
# that the specifics of items differ. (Lists use function arguments since
# Docker does not allow empty list sections: we generate the section name
# only if we have one or more items in that list.)
#
# If your needs are complex, you can create your own per-test functions
# called from the "overriden" ones. To help future readers tell the two
# apart, add the following comment to overrides:
#
# # Override
#
#--------------------------------------------------------------------

# Replace this to create a test-specific header comment.
function gen_header_comment {
	:
}

function gen_header {
	cat << EOF
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

# THIS FILE IS GENERATED -- DO NOT EDIT!
#
# Instead, edit the template from which this file was generated.
# Template: $TEMPLATE

EOF
  	gen_header_comment
}

function gen_networks {
	cat << EOF
networks:
  druid-it-net:
    name: druid-it-net
    ipam:
      config:
        - subnet: 172.172.172.0/24

EOF
}

function dep_service_header {
	service=$1
	base_service=$service
	if [ $# -gt 1 ]; then
		base_service=$1
    fi
	cat << EOF
  $service:
    extends:
      file: ../Common/dependencies.yaml
      service: $base_service
EOF
}

function gen_zk {
	dep_service_header zookeeper
	cat << EOF

EOF
}

function gen_metadata {
	dep_service_header metadata
	cat << EOF

EOF
}

function druid_service_header {
	service=$1
	base_service=$service
	if [ $# -gt 1 ]; then
		base_service=$2
    fi
	cat << EOF
  $service:
    extends:
      file: ../Common/druid.yaml
      service: $base_service
EOF
}

function gen_depends {
	cat << EOF
    depends_on:
      - zookeeper
EOF
    for dep in $*; do
      cat << EOF
      - $dep
EOF
    done
	cat << EOF

EOF
}

# Generate volumes, if any. It is not legal to have a volumes tag
# with no content, so generate the tag only if volumes exist.
function gen_volumes {
	if [ $# -gt 0 ]; then
	    cat << EOF
    volumes:
EOF
        for vol in $*; do
        	cat << EOF
      - $vol
EOF
        done
    fi
}

# Generate the environment, if any. It is not legal to have a environment tag
# with no content, so generate the tag only if environment exist.
function gen_env {
	if [ $# -gt 0 ]; then
	    cat << EOF
    environment:
EOF
        for pair in $*; do
        	cat << EOF
      - $pair
EOF
        done
    fi
}

# Generate env files, if any. It is not legal to have a env_file tag
# with no content, so generate the tag only if env files exist.
function gen_env_files {
	if [ $# -gt 0 ]; then
	    cat << EOF
    env_file:
EOF
        for file in $*; do
        	cat << EOF
      - $file
EOF
        done
    fi
}

function gen_common_volumes {
	gen_volumes $*
}

function gen_common_env_files {
	gen_env_files $*
}

function gen_common_env {
	gen_env $*
}

function gen_coordinator_env_file {
	gen_common_env_files $*
}

function gen_coordinator_env {
	gen_common_env $*
}

function gen_coordinator_volumes {
	gen_common_volumes $*
}

function gen_master_service {
	druid_service_header $1 $2
	gen_$2_env_file
	gen_$2_env
	gen_$2_volumes
	gen_depends metadata
}

function gen_coordinator_service {
	gen_master_service $1 "coordinator"
}

function gen_coordinator {
	gen_coordinator_service "coordinator"
}

function gen_overlord_env_file {
	gen_common_env_files $*
}

function gen_overlord_env {
	gen_common_env $*
}

function gen_overlord_volumes {
	gen_common_volumes $*
}

function gen_overlord_service {
	gen_master_service $1 "overlord"
}

function gen_overlord {
	gen_overlord_service "overlord"
}

function gen_simple_service {
	druid_service_header $1
	gen_$1_env_file
	gen_$1_env
	gen_$1_volumes
	gen_depends
}

function gen_broker_env_file {
	gen_common_env_files $*
}

function gen_broker_env {
	gen_common_env $*
}

function gen_broker_volumes {
	gen_common_volumes $*
}

function gen_broker {
	gen_simple_service "broker"
}

function gen_router_env {
	gen_common_env $*
}

function gen_router_env_file {
	gen_common_env_files $*
}

function gen_router_volumes {
	gen_common_volumes $*
}

function gen_router {
	gen_simple_service "router"
}

function gen_historical_env {
	gen_common_env $*
}

function gen_historical_env_file {
	gen_common_env_files $*
}

function gen_historical_volumes {
	gen_common_volumes $*
}

function gen_historical {
	gen_simple_service "historical"
}

function gen_indexer_env {
	gen_common_env $*
}

function gen_indexer_env_file {
	gen_common_env_files $*
}

function gen_indexer_volumes {
	gen_common_volumes $*
}

function gen_middlemanager_env {
	gen_indexer_env $*
}

function gen_indexer_volumes {
	# Test data
	gen_common_volumes \
		"\${MODULE_DIR}/resources:/resources"
}

function gen_indexer {
	indexer="middlemanager"
	if [ -n "$USE_INDEXER" ]; then
	    if [ "$USE_INDEXER" == "indexer" ]; then
	    	indexer="indexer"
	    elif [ "$USE_INDEXER" == "middleManager" ]; then
	    	indexer="middlemanager" # Note lower case spelling
		else
		  echo "USE_INDEXER must be 'indexer' or 'middleManager' (is '$USE_INDEXER')" 1>&2
		  exit 1
		fi
	fi
	druid_service_header $indexer
	gen_indexer_env_file
	gen_${indexer}_env
	gen_indexer_volumes
	gen_depends
}

function gen_services {
	:
}

function gen_custom_services {
	:
}

function gen_services {
	cat << EOF
services:
EOF
	gen_zk
	gen_metadata
	gen_coordinator
	gen_overlord
	gen_broker
	gen_router
	gen_historical
	gen_indexer
	gen_custom_services
}

function gen_docker {
  	gen_header
  	gen_networks
  	gen_services
}

function usage {
	cat << EOF
USAGE: $0 <category>

Requires a (possibly empty) template in cluster/$category/docker.sh.

Generates into target/$cluster/docker-compose.sh
EOF
}

function gen_compose_file {

	CATEGORY=$1
	if [ -z $CATEGORY ]; then
		usage
		exit 1
	fi

	# Prepare the file
	target_dir=target/cluster/$CATEGORY
	mkdir -p $target_dir
	target="$target_dir/docker-compose.yaml"
	rm -f $target

	# Generate the Docker compose file
	gen_docker >> $target
}
