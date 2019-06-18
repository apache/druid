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

cd /tls

rm -f cert_db.txt
touch cert_db.txt

export DOCKER_IP=$(cat /docker_ip)
export MY_HOSTNAME=$(hostname)
export MY_IP=$(hostname -i)

cat <<EOT > csr.conf
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
CN = ${MY_IP}

[ req_ext ]
subjectAltName = @alt_names
basicConstraints=CA:FALSE,pathlen:0

[ alt_names ]
IP.1 = ${DOCKER_IP}
IP.2 = ${MY_IP}
IP.3 = 127.0.0.1
DNS.1 = ${MY_HOSTNAME}
DNS.2 = localhost

EOT

# Generate a server certificate for this machine
openssl genrsa -out server.key 1024 -sha256
openssl req -new -out server.csr -key server.key -reqexts req_ext -config csr.conf
openssl x509 -req -days 3650 -in server.csr -CA root.pem -CAkey root.key -set_serial 0x22222222 -out server.pem -sha256 -extfile csr.conf -extensions req_ext

# Create a Java keystore containing the generated certificate
openssl pkcs12 -export -in server.pem -inkey server.key -out server.p12 -name druid -CAfile root.pem -caname druid-it-root -password pass:druid123
keytool -importkeystore -srckeystore server.p12 -srcstoretype PKCS12 -destkeystore server.jks -deststoretype JKS -srcstorepass druid123 -deststorepass druid123

# Create a Java truststore with the imply test cluster root CA
keytool -import -alias druid-it-root -keystore truststore.jks -file root.pem -storepass druid123 -noprompt

# Revoke one of the client certs
openssl ca -revoke /client_tls/revoked_client.pem -config root.cnf -cert root.pem -keyfile root.key

# Create the CRL
openssl ca -gencrl -config root.cnf -cert root.pem -keyfile root.key -out /tls/revocations.crl

# Generate empty CRLs for the intermediate cert test case
rm -f cert_db2.txt
touch cert_db2.txt
openssl ca -gencrl -config root2.cnf -cert /client_tls/ca_intermediate.pem -keyfile /client_tls/ca_intermediate.key -out /tls/empty-revocations-intermediate.crl

# Append CRLs
cat empty-revocations-intermediate.crl >> revocations.crl
