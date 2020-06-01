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

cat <<EOT > ca_intermediate.conf
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
emailAddress=intermediate@druid.apache.org
CN = intermediate

[ req_ext ]
subjectAltName = @alt_names
basicConstraints=CA:TRUE,pathlen:1

[ alt_names ]
IP.1 = 9.9.9.9
EOT

# Generate an intermediate certificate
openssl genrsa -out ca_intermediate.key 1024
openssl req -new -out ca_intermediate.csr -key ca_intermediate.key -reqexts req_ext -config ca_intermediate.conf
openssl x509 -req -days 3650 -in ca_intermediate.csr -CA root.pem -CAkey root.key -set_serial 0x33333332 -out ca_intermediate.pem -sha256 -extfile ca_intermediate.conf -extensions req_ext


cat <<EOT > intermediate_ca_client.conf
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
emailAddress=intermediate-client@druid.apache.org
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
openssl genrsa -out intermediate_ca_client.key 1024
openssl req -new -out intermediate_ca_client.csr -key intermediate_ca_client.key -reqexts req_ext -config intermediate_ca_client.conf
openssl x509 -req -days 3650 -in intermediate_ca_client.csr -CA ca_intermediate.pem -CAkey ca_intermediate.key -set_serial 0x33333333 -out intermediate_ca_client.pem -sha256 -extfile intermediate_ca_client.conf -extensions req_ext

# Append the signing cert
printf "\n" >> intermediate_ca_client.pem
cat ca_intermediate.pem >> intermediate_ca_client.pem

# Create a Java keystore containing the generated certificate
openssl pkcs12 -export -in intermediate_ca_client.pem -inkey intermediate_ca_client.key -out intermediate_ca_client.p12 -name intermediate_ca_client -CAfile ca_intermediate.pem -caname druid-it-root -password pass:druid123
keytool -importkeystore -srckeystore intermediate_ca_client.p12 -srcstoretype PKCS12 -destkeystore intermediate_ca_client.jks -deststoretype JKS -srcstorepass druid123 -deststorepass druid123
