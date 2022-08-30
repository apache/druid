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
# commands are unwieldy.
set -e

export DRUID_DEV=$(cd $(dirname $0) && pwd)

function usage
{
  cat <<EOF
Usage: $0 cmd [category]
  build
      build Druid and the distribution
  dist
      build the Druid distribution (only)
  tools
      build druid-it-tools
  image
      build the test image
  up <category>
      start the cluster for category
  down <category>
      stop the cluster for category
  test <category>
      start the cluster, run the test for category, and stop the cluster
  tail <category>
      show the last 20 lines of each container log
  travis <category>
      run one IT in Travis (build dist, image, run test, tail logs)
  prune
      prune Docker volumes
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

CMD=$1
shift
MAVEN_IGNORE="-P skip-static-checks,skip-tests -Dmaven.javadoc.skip=true"

case $CMD in
  "help" )
    usage
    ;;
  "build" )
    mvn clean package -P dist $MAVEN_IGNORE -T1.0C
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
  "up" )
    if [ -z "$1" ]; then
      usage
      exit 1
    fi
    cd $DRUID_DEV/integration-tests-ex/cases
    ./cluster.sh up $1
    ;;
  "down" )
    if [ -z "$1" ]; then
      usage
      exit 1
    fi
    cd $DRUID_DEV/integration-tests-ex/cases
    ./cluster.sh down $1
    ;;
  "test" )
    if [ -z "$1" ]; then
      usage
      exit 1
    fi
    cd $DRUID_DEV/integration-tests-ex/cases
    mvn verify -P skip-static-checks,docker-tests,IT-$1 \
            -Dmaven.javadoc.skip=true -DskipUTs=true \
            -pl :druid-it-cases
    ;;
  "tail" )
    if [ -z "$1" ]; then
      usage
      exit 1
    fi
    tail_logs $1
    ;;
    "travis" )
    if [ -z "$1" ]; then
      usage
      exit 1
    fi
      $0 dist
      $0 image
      $0 test $1
      $0 tail $1
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
