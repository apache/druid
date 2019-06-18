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

export DOCKER_HOST_IP=$(resolveip -s $HOSTNAME)

cat <<EOT > csr_another_root.conf
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
emailAddress=integration-test@druid.apache.org
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
openssl genrsa -out client_another_root.key 1024 -sha256
openssl req -new -out client_another_root.csr -key client_another_root.key -reqexts req_ext -config csr_another_root.conf
openssl x509 -req -days 3650 -in client_another_root.csr -CA untrusted_root.pem -CAkey untrusted_root.key -set_serial 0x11111114 -out client_another_root.pem -sha256 -extfile csr_another_root.conf -extensions req_ext

# Create a Java keystore containing the generated certificate
openssl pkcs12 -export -in client_another_root.pem -inkey client_another_root.key -out client_another_root.p12 -name druid_another_root -CAfile untrusted_root.pem -caname druid-it-untrusted-root -password pass:druid123
keytool -importkeystore -srckeystore client_another_root.p12 -srcstoretype PKCS12 -destkeystore client_another_root.jks -deststoretype JKS -srcstorepass druid123 -deststorepass druid123

