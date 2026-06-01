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

rm -f scripts/root.key
rm -f scripts/untrusted_root.key
rm -f scripts/root.pem
rm -f scripts/untrusted_root.pem

openssl genrsa -out scripts/root.key 4096
openssl genrsa -out scripts/untrusted_root.key 4096

openssl req -config scripts/root.cnf -key scripts/root.key -new -x509 -days 3650 -sha256 -extensions v3_ca -out scripts/root.pem
openssl req -config scripts/root.cnf -key scripts/untrusted_root.key -new -x509 -days 3650 -sha256 -extensions v3_ca -out scripts/untrusted_root.pem

