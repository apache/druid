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

rm -f root.key
rm -f untrusted_root.key
rm -f root.pem
rm -f untrusted_root.pem

openssl genrsa -out docker/tls/root.key 4096
openssl genrsa -out docker/tls/untrusted_root.key 4096

openssl req -config docker/tls/root.cnf -key docker/tls/root.key -new -x509 -days 3650 -sha256 -extensions v3_ca -out docker/tls/root.pem
openssl req -config docker/tls/root.cnf -key docker/tls/untrusted_root.key -new -x509 -days 3650 -sha256 -extensions v3_ca -out docker/tls/untrusted_root.pem

