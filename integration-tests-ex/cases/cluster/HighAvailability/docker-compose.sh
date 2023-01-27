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

# Generates a Docker compose file with two coordinators, two overlords,
# no historical, no indexer and with a custom node role. Functions marked
# with "Override" replace those in the base template (poor man's inhertitance)
# while all others are specific to this test.

set -e

export MODULE_DIR=$(cd $(dirname $0) && pwd)
export CATEGORY=$(basename $MODULE_DIR)

. $MODULE_DIR/../Common/gen-docker.sh

# The first coordinator uses the inherited properties.
function gen_coordinator_one {
	druid_service_header "coordinator-one" "coordinator"
	cat << EOF
    container_name: coordinator-one
EOF
    gen_coordinator_env_file
	gen_coordinator_env \
		"DRUID_INSTANCE=one" \
    	"druid_host=coordinator-one"
    gen_coordinator_volumes
	gen_depends metadata
}

# Generates a header for a service which is not inhertited: where
# we have to spell out the details.
function gen_custom_service {
	base=$1
	service=$2
	port=$3
	cat << EOF
  $service:
    image: \${DRUID_IT_IMAGE_NAME}
    container_name: $service
    networks:
      druid-it-net:
        ipv4_address: 172.172.172.$port
    volumes:
      - \${SHARED_DIR}:/shared
    env_file:
      - ../Common/environment-configs/common.env
EOF
    if [ -n "$base" ]; then
    	cat << EOF
      - ../Common/environment-configs/$base.env
EOF
    fi
    cat << EOF
      - \${OVERRIDE_ENV}
    environment:
      - DRUID_INTEGRATION_TEST_GROUP=\${DRUID_INTEGRATION_TEST_GROUP}
EOF
}

# Generates a second coordinator as a custom service since we can't
# reuse the standard ports.
function gen_coordinator_two {
	cat << EOF
  # The second Coordinator (and Overlord) cannot extend
  # The base service: they need distinct ports.
EOF
	gen_custom_service "coordinator" "coordinator-two" 120
	cat << EOF
      - DRUID_INSTANCE=two
      - druid_host=coordinator-two
    ports:
      - 18081:8081
      - 18281:8281
      - 15006:8000
EOF
	gen_depends metadata
}

# Generate the coordinator for this test which is, in fact, two nodes.
# Override
function gen_coordinator {
	gen_coordinator_one
	gen_coordinator_two
}

# Generate the first overlord using the inherited properties.
function gen_overlord_one {
	druid_service_header "overlord-one" "overlord"
	cat << EOF
    container_name: overlord-one
EOF
    gen_overlord_env_file
	gen_overlord_env \
      "DRUID_INSTANCE=one" \
      "druid_host=overlord-one"
    gen_overlord_volumes
	gen_depends metadata
}

# Generate a second overlord as a custom service since we can't
# reuse the standard ports.
function gen_overlord_two {
	gen_custom_service "overlord" "overlord-two" 110
	cat << EOF
      - DRUID_INSTANCE=two
      - druid_host=overlord-two
    ports:
      - 18090:8090
      - 18290:8290
      - 15009:8000
EOF
	gen_depends metadata
}

# Generate the overlord for this test which is, in fact, two nodes.
# Override
function gen_overlord {
	gen_overlord_one
	gen_overlord_two
}

# No historical for this cluster
# Override
function gen_historical {
	: # No historical
}

# No indexer for this cluster
# Override
function gen_indexer {
	: # No indexer
}

# Generate a custom node rule as a custom service, with custom ports and
# a set of service-specific properties
function gen_custom_node_role {
	cat << EOF
  # The custom node role has no base definition. Also, there is
  # no environment file: the needed environment settings are
  # given here.
EOF
	gen_custom_service "" "custom-node-role" 90
	cat << EOF
      - DRUID_SERVICE=custom-node-role
      - SERVICE_DRUID_JAVA_OPTS=-Xmx64m -Xms64m
      - druid_host=custom-node-role
      - druid_auth_basic_common_cacheDirectory=/tmp/authCache/custom_node_role
      - druid_server_https_crlPath=/tls/revocations.crl
    ports:
      - 50011:50011
      - 9301:9301
      - 9501:9501
      - 5010:8000
EOF
	gen_common_volumes
}

# Generate the custom node role
# Overide
function gen_custom_services {
	gen_custom_node_role
}

gen_compose_file $CATEGORY
