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

mkdir -p server_tls
rm -f server_tls/*

cp scripts/root.cnf server_tls/root.cnf
cp scripts/root2.cnf server_tls/root2.cnf

cp scripts/root.key server_tls/root.key
cp scripts/root.pem server_tls/root.pem
cp scripts/untrusted_root.key server_tls/untrusted_root.key
cp scripts/untrusted_root.pem server_tls/untrusted_root.pem

FILE_CHECK_IF_RAN=server_tls/server.key
if [ -f "$FILE_CHECK_IF_RAN" ]; then
  echo "Using existing certs/keys since server_tls/server.key exists. Skipping generation (most likely this script was ran previously). To generate new certs, delete server_tls/server.key"
  exit
fi

cd server_tls
rm -f cert_db.txt
touch cert_db.txt

export MY_HOSTNAME=$(hostname)

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
CN = 127.0.0.1

[ req_ext ]
subjectAltName = @alt_names
basicConstraints=CA:FALSE,pathlen:0

[ alt_names ]
IP.1 = 127.0.0.1
DNS.1 = ${MY_HOSTNAME}
DNS.2 = localhost

EOT

# Generate a server certificate for this machine
openssl genrsa -out server.key 4096
openssl req -new -out server.csr -key server.key -reqexts req_ext -config csr.conf
openssl x509 -req -days 3650 -in server.csr -CA root.pem -CAkey root.key -set_serial 0x22222222 -out server.pem -sha256 -extfile csr.conf -extensions req_ext

# Create a Java keystore containing the generated certificate in PKCS12 format
openssl pkcs12 -export -in server.pem -inkey server.key -out server.p12 -name druid -CAfile root.pem -caname druid-it-root -password pass:druid123

# Create a Java truststore with the druid test cluster root CA
keytool -import -alias druid-it-root -keystore truststore.jks -file root.pem -storepass druid123 -noprompt

# Revoke one of the client certs
openssl ca -revoke ../client_tls/revoked_client.pem -config root.cnf -cert root.pem -keyfile root.key

# Create the CRL
openssl ca -gencrl -config root.cnf -cert root.pem -keyfile root.key -out revocations.crl

# Generate empty CRLs for the intermediate cert test case
rm -f cert_db2.txt
touch cert_db2.txt
openssl ca -gencrl -config root2.cnf -cert ../client_tls/ca_intermediate.pem -keyfile ../client_tls/ca_intermediate.key -out empty-revocations-intermediate.crl

# Append CRLs
cat empty-revocations-intermediate.crl >> revocations.crl
