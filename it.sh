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

export DRUID_DEV=$(cd $(dirname $0) && pwd)

function usage {
	echo "Usage: $0 cmd [category]"
	echo "  dist            - build the Druid distribution"
	echo "  image           - build the test image"
	echo "  up <category>   - start the cluster for category"
	echo "  down <category> - stop the cluster for category"
	echo "  test <category> - start the cluster, run the test for category, and stop the cluster"
}

CMD=$1
shift
MAVEN_IGNORE="-P skip-static-checks,skip-tests -Dmaven.javadoc.skip=true"

case $CMD in
	"dist")
		mvn clean package -P dist $MAVEN_IGNORE -T1.0C
		;;
	"image")
		cd $DRUID_DEV/integration-tests-ex/image
		mvn install -P test-image $MAVEN_IGNORE
		;;
	"up")
		if [ -z "$1" ]; then
			usage
			exit 1
		fi
		cd $DRUID_DEV/integration-tests-ex/cases
		./cluster.sh $1 up
		;;
	"down")
		if [ -z "$1" ]; then
			usage
			exit 1
		fi
		cd $DRUID_DEV/integration-tests-ex/cases
		./cluster.sh $1 down
		;;
	"test")
		if [ -z "$1" ]; then
			usage
			exit 1
		fi
		cd $DRUID_DEV/integration-tests-ex/cases
		mvn verify -P skip-static-checks,docker-tests,IT-$1 \
            -Dmaven.javadoc.skip=true -DskipUTs=true \
            -pl :druid-it-cases
		;;
 	"help")
 		usage
 		;;
	*)
		usage
		exit -1
esac
