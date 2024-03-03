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

# Set up for the gRPC query tests.
# 1. Unpack the grpc-query extension into a suplemental extensions directory:
#    /target/GrpcQuery/extensions. The container launch.sh script will add
#    this directory to Druid's extensions path when it sees that the directory
#    exists.
# 2. Add the test Protobuf jar to the extension created above. In a real
#    deployment, application-specific Protobuf messages would be added to the
#    extension. We simulate that here with the same message used for unit tests.
#
# The final layout will be:
# <module>/target/GrpcQuery
#   - extensions
#     - grpc-query
#       - grpc-query-<version>.jar
#       - druid-shaded-grpc-<version>.jar
#       - grpc-query-<version>-test-proto.jar

#set -x
echo "shared dir is $SHARED_DIR"
EXTENSION_DIR=$SHARED_DIR/extensions
mkdir -p $EXTENSION_DIR

# Expand the grpc-query artifact into the extensions directory
GRPC_QUERY_DIR=$MODULE_DIR/../grpc-query
cd $EXTENSION_DIR
tar -xzf $GRPC_QUERY_DIR/target/grpc-query-*.tar.gz
GPRC_EXTN_DIR=$EXTENSION_DIR/grpc-query
if [ ! -d $GPRC_EXTN_DIR ]; then
  echo "Something went wrong: grpc-query didn't unpack to ${GPRC_EXTN_DIR}!" 1>&2
  exit 1
fi

# Add the jar with the Protobuf classes
cp $GRPC_QUERY_DIR/target/grpc-query-*-test-proto.jar $GPRC_EXTN_DIR
