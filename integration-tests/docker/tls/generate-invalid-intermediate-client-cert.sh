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

tls_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=set-docker-host-ip.sh
source "$tls_dir/set-docker-host-ip.sh"

cat <<EOT > invalid_ca_intermediate.conf
[req]
default_bits = 1024
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
C=DR
ST=DR
L=Druid City
O=Druid
OU=IntegrationTests
emailAddress=bad-intermediate@druid.apache.org
CN = badintermediate

[ req_ext ]
subjectAltName = @alt_names
basicConstraints=CA:FALSE,pathlen:0

[ alt_names ]
IP.1 = 9.9.9.9
EOT

# Generate a bad intermediate certificate
openssl genrsa -out invalid_ca_intermediate.key 1024
openssl req -new -out invalid_ca_intermediate.csr -key invalid_ca_intermediate.key -reqexts req_ext -config invalid_ca_intermediate.conf
openssl x509 -req -days 3650 -in invalid_ca_intermediate.csr -CA root.pem -CAkey root.key -set_serial 0x33333331 -out invalid_ca_intermediate.pem -sha256 -extfile invalid_ca_intermediate.conf -extensions req_ext


cat <<EOT > invalid_ca_client.conf
[req]
default_bits = 1024
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
C=DR
ST=DR
L=Druid City
O=Druid
OU=IntegrationTests
emailAddress=basic-constraint-fail@druid.apache.org
CN = localhost

[ req_ext ]
subjectAltName = @alt_names
basicConstraints=CA:FALSE,pathlen:0

[ alt_names ]
IP.1 = ${DOCKER_HOST_IP}
IP.2 = 127.0.0.1
IP.3 = 172.172.172.1
IP.4 = ${DOCKER_MACHINE_IP:=127.0.0.1}
DNS.1 = ${HOSTNAME}
DNS.2 = localhost
EOT

# Generate a client certificate for this machine
openssl genrsa -out invalid_ca_client.key 1024
openssl req -new -out invalid_ca_client.csr -key invalid_ca_client.key -reqexts req_ext -config invalid_ca_client.conf
openssl x509 -req -days 3650 -in invalid_ca_client.csr -CA invalid_ca_intermediate.pem -CAkey invalid_ca_intermediate.key -set_serial 0x33333333 -out invalid_ca_client.pem -sha256 -extfile invalid_ca_client.conf -extensions req_ext

# Append the signing cert
printf "\n" >> invalid_ca_client.pem
cat invalid_ca_intermediate.pem >> invalid_ca_client.pem

# Create a Java keystore containing the generated certificate
openssl pkcs12 -export -in invalid_ca_client.pem -inkey invalid_ca_client.key -out invalid_ca_client.p12 -name invalid_ca_client -CAfile invalid_ca_intermediate.pem -caname druid-it-root -password pass:druid123
keytool -importkeystore -srckeystore invalid_ca_client.p12 -srcstoretype PKCS12 -destkeystore invalid_ca_client.jks -deststoretype JKS -srcstorepass druid123 -deststorepass druid123
