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
Usage: $0 cmd [category] [module]
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
  up <category> [<module>]
      Start the cluster for category.
  down <category> [<module>]
      Stop the cluster for category.
  run <category> [<module>]
      Run the tests for the given module on an alread-running cluster.
      Does not stop the cluster. Primarily for debugging.
  test <category> [<module>]
      Start the cluster, run the test for category, and stop the cluster.
  tail <category> [<module>]
      Show the last 20 lines of each container log.
  gen <category> [<module>]
      Generate docker-compose.yaml files (done automatically on up)
      run one IT in Travis (build dist, image, run test, tail logs).
  github <category> [<module>]
      Run one IT in Github Workflows (run test, tail logs).
  prune-containers
      Stop all running Docker containers. Do this if "down" won't work
      because the "docker-compose.yaml" file is no longer available.
  prune-volumes
      prune Docker volumes.

Arguments:
  category: A defined IT JUnit category, and IT-<category> profile
  module: relative path to the module with tests. Defaults to
          integration-tests-ex/cases

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
  pushd $MODULE_DIR/target/$CATEGORY/logs > /dev/null
  ls *.log | while read log;
  do
    echo "----- $CATEGORY/$log -----"
    tail -100 $log
  done
  popd > /dev/null
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
#
# The file is built when the cluster comes up. It is reused in the test and down
# commands so we have a consistent environment.
function build_override {

	mkdir -p "$MODULE_DIR/target"
	OVERRIDE_FILE="$MODULE_DIR/target/override.env"
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

function reuse_override {
  OVERRIDE_FILE="$MODULE_DIR/target/override.env"
  if [ ! -f "$OVERRIDE_FILE" ]; then
      echo "Override file $OVERRIDE_FILE not found: Was an 'up' run?" 1>&2
      exit 1
  fi
  export OVERRIDE_ENV="$OVERRIDE_FILE"
}

function require_category {
	if [ -z "$CATEGORY" ]; then
		usage 1>&2
		exit 1
	fi
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
  VERIFY_SCRIPT="$MODULE_DIR/cluster/$DRUID_INTEGRATION_TEST_GROUP/verify.sh"
  if [ -f "$VERIFY_SCRIPT" ]; then
    . "$VERIFY_SCRIPT"
  fi
}

if [ $# -eq 0 ]; then
  usage
  exit 1
fi

CMD=$1
shift
if [ $# -gt 0 ]; then
	CATEGORY=$1
	shift
fi

# Handle an IT in either the usual druid-it-cases project, or elsewhere,
# typically in an extension. The Maven module, if needed must be the third
# parameter in path, not coordinate, form.
if [ $# -eq 0 ]; then
  # Use the usual project
  MAVEN_PROJECT=":druid-it-cases"
  # Don't provide a project path to cluster.sh
  unset IT_MODULE_DIR
  # Generate the override.sh file in the druid-it-cases module
  MODULE_DIR=$DRUID_DEV/integration-tests-ex/cases
else
  # The test module is given via the command line argument as a relative path
  MAVEN_PROJECT="$1"
  # Compute the full path to the target module for use by cluster.sh
  export IT_MODULE_DIR="$DRUID_DEV/$1"
  # Write the override.sh file to the target module
  MODULE_DIR=$IT_MODULE_DIR
  shift
fi

IT_CASES_DIR="$DRUID_DEV/integration-tests-ex/cases"

# Added -Dcyclonedx.skip=true to avoid ISO-8859-1 [ERROR]s
# May be fixed in the future
MAVEN_IGNORE="-P skip-static-checks,skip-tests -Dmaven.javadoc.skip=true -Dcyclonedx.skip=true"
TEST_OPTIONS="verify -P skip-static-checks,docker-tests \
            -Dmaven.javadoc.skip=true -Dcyclonedx.skip=true -DskipUTs=true"

case $CMD in
  "help" )
    usage
    ;;
  "ci" )
    mvn -q clean install dependency:go-offline -P dist $MAVEN_IGNORE
    ;;
  "build" )
    mvn -B clean install -P dist $MAVEN_IGNORE -T1.0C $*
    ;;
  "dist" )
    mvn -B install -P dist $MAVEN_IGNORE -pl :distribution
    ;;
  "tools" )
    mvn -B install -pl :druid-it-tools
    ;;
  "image" )
    cd $DRUID_DEV/integration-tests-ex/image
    mvn -B install -P test-image $MAVEN_IGNORE
    ;;
  "gen")
    # Generate the docker-compose.yaml files. Mostly for debugging
    # since the up command does generation implicitly.
    require_category
    $IT_CASES_DIR/cluster.sh gen $CATEGORY
    ;;
  "up" )
    require_category
    build_override
    verify_env_vars
    $IT_CASES_DIR/cluster.sh up $CATEGORY
    ;;
  "down" )
    require_category
    reuse_override
    $IT_CASES_DIR/cluster.sh down $CATEGORY
    ;;
  "run" )
    require_category
    reuse_override
    mvn -B $TEST_OPTIONS -P IT-$CATEGORY -pl $MAVEN_PROJECT
    ;;
  "test" )
    require_category
    build_override
    verify_env_vars
    $IT_CASES_DIR/cluster.sh up $CATEGORY

    # Run the test. On failure, still shut down the cluster.
    # Return Maven's return code as the script's return code.
    set +e
    mvn -B $TEST_OPTIONS -P IT-$CATEGORY -pl $MAVEN_PROJECT
    RESULT=$?
    set -e
    $IT_CASES_DIR/cluster.sh down $CATEGORY
    exit $RESULT
    ;;
  "tail" )
    require_category
    tail_logs
    ;;
  "github" )
    set +e
    $0 test $CATEGORY
    RESULT=$?

    # Include logs, but only for failures.
    if [ $RESULT -ne 0 ]; then
      $0 tail $CATEGORY
    fi
    exit $RESULT
    ;;
  # Name is deliberately long to avoid accidental use.
  "prune-containers" )
    if [ $(docker ps | wc -l) -ne 1 ]; then
      echo "Cleaning running containers"
      docker ps
      docker ps -aq | xargs -r docker rm -f
    fi
    ;;
  "prune-volumes" )
    # Caution: this removes all volumes, which is generally what you
    # want when testing.
    docker system prune -af --volumes
    ;;
  * )
    usage
    exit 1
    ;;
esac
