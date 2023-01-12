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
# DRUID_INTEGRATION_TEST_INDEXER env var.
#
# This script acts like a simple template engine. Since the generated
# files are simple, using shell commands for the template is not quite
# as crazy as it sounds. If needes get more complex, we can switch to
# the Freemarker template engine used to generate the Calcite parser.
#
# To customize, find the function that generates the bit to be changed.
# Copy that function to your per-test template. This master template
# includes the per-test template after defining all functions, so that
# any in your template replace (not override) those defined here.
#
# Any function with "custom" in the name is intended to help with
# customization. The others are "boilerplate" and generally should not
# need to be replaced.

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

function gen_custom_env {
	:
}

function gen_common_env {
	cat << EOF
    environment:
      - DRUID_INTEGRATION_TEST_GROUP=\${DRUID_INTEGRATION_TEST_GROUP}
EOF
	gen_custom_env
}

function gen_coord_env {
	cat << EOF
      # The frequency with which the coordinator polls the database
      # for changes. The DB population code has to wait at least this
      # long for the coordinator to notice changes.
      - druid_manager_segments_pollDuration=PT5S
      - druid_coordinator_period=PT10S
EOF
}

function gen_custom_coord_env {
	:
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

function gen_druid_volumes {
	cat << EOF
    volumes:
EOF
}

function gen_coord_volumes {
	:
}

function gen_coord_service {
	druid_service_header $1 "coordinator"
	gen_common_env
	gen_coord_env
	gen_custom_coord_env
	gen_druid_volumes
	gen_coord_volumes
	gen_depends metadata
}

function gen_coordinator {
	gen_coord_service "coordinator"
}

function gen_overlord_env {
	:
}

function gen_custom_overlord_env {
	:
}

function gen_overlord_volumes {
	:
}

function gen_overlord_service {
	druid_service_header $1 "overlord"
	gen_common_env
	gen_overlord_env
	gen_custom_overlord_env
	gen_druid_volumes
	gen_overlord_volumes
	gen_depends metadata
}

function gen_overlord {
	gen_overlord_service "overlord"
}

function gen_simple_service {
	druid_service_header $1
	gen_common_env
	gen_custom_$1_env
	gen_druid_volumes
	gen_$1_volumes
	gen_depends
}

function gen_custom_broker_env {
	:
}

function gen_broker {
	gen_simple_service "broker"
}

function gen_custom_router_env {
	:
}

function gen_broker_volumes {
	:
}

function gen_router {
	gen_simple_service "router"
}

function gen_router_volumes {
	:
}

function gen_custom_historical_env {
	:
}

function gen_historical {
	gen_simple_service "historical"
}

function gen_historical_volumes {
	:
}

function gen_custom_indexer_env {
	:
}

function gen_indexer_volumes {
	:
}

function gen_custom_middlemanager_env {
	gen_custom_indexer_env
}

function gen_indexer_volumes {
	cat << EOF
      # Test data
      - ../../resources:/resources
EOF
}

function gen_indexer {
	indexer="middlemanager"
	if [ -n "$DRUID_INTEGRATION_TEST_INDEXER" ]; then
	    if [ "$DRUID_INTEGRATION_TEST_INDEXER" == "indexer" ]; then
	    	indexer="indexer"
	    elif [ "$DRUID_INTEGRATION_TEST_INDEXER" == "middleManager" ]; then
	    	indexer="middlemanager" # Note lower case spelling
		else
		  echo "DRUID_INTEGRATION_TEST_INDEXER must be 'indexer' or 'middleManager' (is '$DRUID_INTEGRATION_TEST_INDEXER')" 1>&2
		  exit 1
		fi
	fi
	druid_service_header $indexer
	gen_common_env
	gen_custom_${indexer}_env
	gen_druid_volumes
	gen_indexer_volumes
	gen_depends
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
