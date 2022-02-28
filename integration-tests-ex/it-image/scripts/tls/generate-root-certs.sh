#!/bin/bash -eu

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

# Run with the project directory as the cwd

if [ -z "$SERVER_DIR" ]; then
	echo "SERVER_DIR is not set!"
	exit 1
fi

mkdir -p $SERVER_DIR

rm -f $SERVER_DIR/root.key
rm -f $SERVER_DIR/untrusted_root.key
rm -f $SERVER_DIR/root.pem
rm -f $SERVER_DIR/untrusted_root.pem

genRootKey() {
  openssl genrsa -out $SERVER_DIR/$1.key 4096
  openssl req -config $SOURCE_DIR/root.cnf -key $SERVER_DIR/$1.key -new -x509 -days 3650 \
          -sha256 -extensions v3_ca -out $SERVER_DIR/$1.pem
}

genRootKey root
genRootKey untrusted_root
