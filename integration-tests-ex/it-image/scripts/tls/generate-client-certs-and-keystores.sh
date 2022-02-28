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

export SOURCE_DIR="$(cd `dirname $0` && pwd)"
SHARED_DIR=target/shared
export SERVER_DIR=$SHARED_DIR/tls
export CLIENT_DIR=$SHARED_DIR/client_tls

$SOURCE_DIR/generate-root-certs.sh

mkdir -p $CLIENT_DIR
rm -f $CLIENT_DIR/*
cp $SERVER_DIR/root.key $CLIENT_DIR/root.key
cp $SERVER_DIR/root.pem $CLIENT_DIR/root.pem
cp $SERVER_DIR/untrusted_root.key $CLIENT_DIR/untrusted_root.key
cp $SERVER_DIR/untrusted_root.pem $CLIENT_DIR/untrusted_root.pem

source "$SOURCE_DIR/set-docker-host-ip.sh"

cd $CLIENT_DIR
$SOURCE_DIR/generate-expired-client-cert.sh
$SOURCE_DIR/generate-good-client-cert.sh
$SOURCE_DIR/generate-incorrect-hostname-client-cert.sh
$SOURCE_DIR/generate-invalid-intermediate-client-cert.sh
$SOURCE_DIR/generate-to-be-revoked-client-cert.sh
$SOURCE_DIR/generate-untrusted-root-client-cert.sh
$SOURCE_DIR/generate-valid-intermediate-client-cert.sh
