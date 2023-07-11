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

# Utility script for running the new integration tests, since the Maven
# commands are unwieldy. Allows straightforward usage of ITs on the desktop
# and in various build scripts. Handles configuration of various kinds.

set -e

# Enable for debugging
#set -x

export DRUID_DEV=$(cd $(dirname $0) && pwd)

function usage
{
  cat <<EOF
Usage: $0 cmd [category]
  ci
      build Druid and the distribution for CI pipelines
  build
      Build Druid and the distribution
  dist
      Build the Druid distribution (only)
  tools
      Build druid-it-tools
  image
      Build the test image
  up <category>
      Start the cluster for category
  down <category>
      Stop the cluster for category
  test <category>
      Start the cluster, run the test for category, and stop the cluster
  tail <category>
      Show the last 20 lines of each container log
  gen
      Generate docker-compose.yaml files (done automatically on up)
      run one IT in Travis (build dist, image, run test, tail logs)
  github <category>
      Run one IT in Github Workflows (run test, tail logs)
  prune
      prune Docker volumes

Environment:
  OVERRIDE_ENV: optional, name of env file to pass to Docker
  USE_INDEXER: Set to middleManager (default if not set)
      or "indexer". If "indexer", requires docker-compose-indexer.yaml exist.
  druid_*: passed to the container.
  Other, test-specific variables.

See docs for additional details.
EOF
}

function tail_logs
{
  category=$1
  cd integration-tests-ex/cases/target/$category/logs
  ls *.log | while read log;
  do
    echo "----- $category/$log -----"
    tail -20 $log
  done
}

# Many tests require us to pass information into containers using environment variables.
# The Docker environment is distinct from the environment running this script. We bridge
# the two by passing into Docker compose a file that contains all env vars we want to
# "export" from our local environment into the container environment.
# There are three ways to provide these options:
#
# 1. Directly in the environment. (Simplest and best.) We support a fixed set of variables:
#    <need the list>
# 2. For ad-hoc use, as var=value pairs in a file with the same name as the
#    test catagory, in the home folder under ~/druid-it. Example:
#    BatchIndex.env. Use this to hold credentials and other info which you must
#    pass into tests when running locally.
# 3. A file given by the OVERRIDE_ENV environment variable. That is, OVERRIDE_ENV holds
#    the path to a file of var=value pairs. Historically, this file was created by a
#    build environment such as Github Actions. However, it is actually simpler just to use
#    option 1: just set the values in the environment and let Linux pass them through to
#    this script.
# 4. Environment variables of the form "druid_" used to create the Druid config file.
#
# All of the above are combined into a temporary environment file which is then passed
# into Docker compose.
function build_override {

	mkdir -p target
	OVERRIDE_FILE="$(pwd)/target/override.env"
	rm -f "$OVERRIDE_FILE"
	touch "$OVERRIDE_FILE"

	# Provided override file
	if [ -n "$OVERRIDE_ENV" ]; then
		if [ ! -f "$OVERRIDE_ENV" ]; then
			echo "Environment override file OVERRIDE_ENV not found: $OVERRIDE_ENV" 1>&2
			exit 1
		fi
		cat "$OVERRIDE_ENV" >> "$OVERRIDE_FILE"
	fi

	# User-local settings?
	LOCAL_ENV="$HOME/druid-it/${CATEGORY}.env"
	if [ -f "$LOCAL_ENV" ]; then
		cat "$LOCAL_ENV" >> "$OVERRIDE_FILE"
	fi

    # Add all environment variables of the form druid_*
    set +e # Grep gives exit status 1 if no lines match. Let's not fail.
    env | grep "^druid_" >> "$OVERRIDE_FILE"
    set -e

    # TODO: Add individual env vars that we want to pass from the local
    # environment into the container.

    # Reuse the OVERRIDE_ENV variable to pass the full list to Docker compose
    export OVERRIDE_ENV="$OVERRIDE_FILE"
}

function prepare_category {
	if [ $# -eq 0 ]; then
		usage 1>&2
		exit 1
	fi
	export CATEGORY=$1
}

function prepare_docker {
    cd $DRUID_DEV/integration-tests-ex/cases
    build_override
}

function require_env_var {
	if [ -z "$1" ]; then
	  echo "$1 must be set for test category $CATEGORY" 1>&2
	  exit 1
    fi
}

# Verfiy any test-specific environment variables that must be set in this local
# environment (and generally passed into the Docker container via docker-compose.yaml).
#
# Add entries here as you add env var references in docker-compose.yaml. Doing so
# ensures we get useful error messages when we forget to set something, rather than
# some cryptic use-specific error.
function verify_env_vars {
	case $CATEGORY in
		"AzureDeepStorage")
			require_env_var AZURE_ACCOUNT
			require_env_var AZURE_KEY
			require_env_var AZURE_CONTAINER
			;;
		"GcsDeepStorage")
			require_env_var GOOGLE_BUCKET
			require_env_var GOOGLE_PREFIX
			require_env_var GOOGLE_APPLICATION_CREDENTIALS
			if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
				echo "Required file GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS is missing" 1>&2
				exit 1
		    fi
			;;
		"S3DeepStorage")
			require_env_var DRUID_CLOUD_BUCKET
			require_env_var DRUID_CLOUD_PATH
			require_env_var AWS_REGION
			require_env_var AWS_ACCESS_KEY_ID
			require_env_var AWS_SECRET_ACCESS_KEY
			;;
	esac
}

if [ $# = 0 ]; then
  usage
  exit 1
fi

CMD=$1
shift
MAVEN_IGNORE="-P skip-static-checks,skip-tests -Dmaven.javadoc.skip=true"

case $CMD in
  "help" )
    usage
    ;;
  "ci" )
    mvn -q clean package dependency:go-offline -P dist $MAVEN_IGNORE
    ;;
  "build" )
    mvn clean package -P dist $MAVEN_IGNORE -T1.0C $*
    ;;
  "dist" )
    mvn package -P dist $MAVEN_IGNORE -pl :distribution
    ;;
  "tools" )
    mvn install -pl :druid-it-tools
    ;;
  "image" )
    cd $DRUID_DEV/integration-tests-ex/image
    mvn install -P test-image $MAVEN_IGNORE
    ;;
  "gen")
    # Generate the docker-compose.yaml files. Mostly for debugging
    # since the up command does generation implicitly.
    prepare_category $1
    prepare_docker
    ./cluster.sh gen $CATEGORY
    ;;
  "up" )
    prepare_category $1
    prepare_docker
    verify_env_vars
    ./cluster.sh up $CATEGORY
    ;;
  "down" )
    prepare_category $1
    prepare_docker
    ./cluster.sh down $CATEGORY
    ;;
  "test" )
    prepare_category $1
    prepare_docker
    mvn verify -P skip-static-checks,docker-tests,IT-$CATEGORY \
            -Dmaven.javadoc.skip=true -DskipUTs=true \
            -pl :druid-it-cases
    ;;
  "tail" )
    prepare_category $1
    tail_logs $CATEGORY
    ;;
  "github" )
    prepare_category $1
    $0 test $CATEGORY
    $0 tail $CATEGORY
    ;;
  "prune" )
    # Caution: this removes all volumes, which is generally what you
    # want when testing.
    docker system prune --volumes
    ;;
  * )
    usage
    exit -1
    ;;
esac
