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
#-------------------------------------------------------------------------

# This script runs inside the container to prepare the Druid test image.

# Fail fast on any error
set -e

# Fail on unset environment variables
set -u

# Enable for tracing
#set -x

# For debugging: verify environment
#env

# Druid system user
adduser --system --group --no-create-home druid

# Adjust ownership of the Druid launch script.
cd /
chmod +x launch.sh
chown druid:druid launch.sh druid.sh

# Convenience script to run Druid for tools.
# Expands the env vars into the script for stability.
# Maybe not needed now?
cat > /run-druid.sh << EOF
#! /bin/bash

java -cp "${DRUID_HOME}/lib/*" \\
	-Ddruid.extensions.directory=${DRUID_HOME}/extensions \\
	-Ddruid.extensions.loadList='["mysql-metadata-storage"]' \\
	-Ddruid.metadata.storage.type=mysql \\
	-Ddruid.metadata.mysql.driver.driverClassName=$MYSQL_DRIVER_CLASSNAME \\
	\$*
EOF
chmod a+x /run-druid.sh

# Install Druid, owned by user:group druid:druid
# The original Druid directory contains only
# libraries. No extensions should be present: those
# should be added in this step.
cd /usr/local/

tar -xzf apache-druid-${DRUID_VERSION}-bin.tar.gz
rm apache-druid-${DRUID_VERSION}-bin.tar.gz

ls -l /tmp/druid

# Add extra libraries and extensions.
mv /tmp/druid/lib/* apache-druid-${DRUID_VERSION}/lib
mv /tmp/druid/extensions/* apache-druid-${DRUID_VERSION}/extensions

# The whole shebang is owned by druid.
chown -R druid:druid apache-druid-${DRUID_VERSION}

# Leave the versioned directory, create a symlink to $DRUID_HOME.
ln -s apache-druid-${DRUID_VERSION} $DRUID_HOME

# Clean up time
# Should be nothing to clean...
rm -rf /tmp/*
rm -rf /var/tmp/*
