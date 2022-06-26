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

# Rebuilds the docker image outside of Maven for
# debugging. Maven sets environment variables, then calls
# build-image.sh which creates the target/env.sh file.
# Here we reuse those environment variables by
# hand.

SCRIPT_DIR=$(cd $(dirname $0) && pwd)

# Target directory. Maven ${project.build.directory}
# Example is for the usual setup.
export TARGET_DIR=$SCRIPT_DIR/target

if [ ! -f $TARGET_DIR/env.sh ]; then
	echo "Please run mvn -P test-image install once before rebuilding" 1>&2
	exit 1
fi

source $TARGET_DIR/env.sh

# Directory of the parent Druid pom.xml file.
# Unbeliebably hard to get from Maven itself.
export PARENT_DIR=$SCRIPT_DIR/../..

exec bash $SCRIPT_DIR/docker-build.sh
