#! /usr/bin/env bash
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

# Prepare the image contents and build the Druid image.
# Since Docker requires all contents to be in or below the
# working directory, we assemble the contents in target/docker.

# Fail fast on any error
set -e

# Enable for tracing
#set -x

# Fail on unset environment variables
set -u

SCRIPT_DIR=$(cd $(dirname $0) && pwd)

# Copy environment variables to a file. Used for manual rebuilds
# and by scripts that start the test cluster.

cat > $TARGET_DIR/env.sh << EOF
export ZK_VERSION=$ZK_VERSION
export KAFKA_VERSION=$KAFKA_VERSION
export DRUID_VERSION=$DRUID_VERSION
export MYSQL_VERSION=$MYSQL_VERSION
export MYSQL_IMAGE_VERSION=$MYSQL_IMAGE_VERSION
export CONFLUENT_VERSION=$CONFLUENT_VERSION
export MARIADB_VERSION=$MARIADB_VERSION
export HADOOP_VERSION=$HADOOP_VERSION
export MYSQL_DRIVER_CLASSNAME=$MYSQL_DRIVER_CLASSNAME
export DRUID_IT_IMAGE_NAME=$DRUID_IT_IMAGE_NAME
EOF

exec bash $SCRIPT_DIR/docker-build.sh
