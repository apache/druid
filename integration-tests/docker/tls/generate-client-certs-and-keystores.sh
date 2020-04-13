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

./docker/tls/generate-root-certs.sh

mkdir -p client_tls
rm -f client_tls/*
cp docker/tls/root.key client_tls/root.key
cp docker/tls/root.pem client_tls/root.pem
cp docker/tls/untrusted_root.key client_tls/untrusted_root.key
cp docker/tls/untrusted_root.pem client_tls/untrusted_root.pem
cd client_tls

../docker/tls/generate-expired-client-cert.sh
../docker/tls/generate-good-client-cert.sh
../docker/tls/generate-incorrect-hostname-client-cert.sh
../docker/tls/generate-invalid-intermediate-client-cert.sh
../docker/tls/generate-to-be-revoked-client-cert.sh
../docker/tls/generate-untrusted-root-client-cert.sh
../docker/tls/generate-valid-intermediate-client-cert.sh


