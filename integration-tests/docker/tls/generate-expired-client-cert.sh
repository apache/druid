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

cat <<EOT > expired_csr.conf
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

cat <<EOT > root_for_expired_client.cnf
[ ca ]
default_ca = CA_default

[ CA_default ]
database = cert_db.txt
x509_extensions	= usr_cert
name_opt = ca_default
cert_opt = ca_default
default_days = 365
default_crl_days= 30
default_md = default
preserve = no
policy = policy_match
serial = certs.seq
email_in_dn=integration-test@druid.apache.org

[req]
default_bits = 4096
prompt = no
default_md = sha256
req_extensions = v3_ca
distinguished_name = dn

[ dn ]
C=DR
ST=DR
L=Druid City
O=Druid
OU=IntegrationTests
emailAddress=integration-test@druid.apache.org
CN = itroot

[ v3_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, digitalSignature, cRLSign, keyCertSign
subjectAltName = @alt_names

[ usr_cert ]

[ policy_loose ]
countryName             = optional
stateOrProvinceName     = optional
localityName            = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional

[ alt_names ]
IP.1 = ${DOCKER_HOST_IP}
IP.2 = 127.0.0.1
IP.3 = 172.172.172.1
IP.4 = ${DOCKER_MACHINE_IP:=127.0.0.1}
DNS.1 = ${HOSTNAME}
DNS.2 = localhost
EOT

rm -f cert_db.txt
touch cert_db.txt
rm -f cert_db.txt.attr
touch cert_db.txt.attr

rm -rf certs.seq
echo 11111115 > certs.seq

# Generate a client certificate for this machine
openssl genrsa -out expired_client.key 1024
openssl req -new -out expired_client.csr -key expired_client.key -reqexts req_ext -config expired_csr.conf
openssl ca -batch -config root_for_expired_client.cnf -policy policy_loose -out expired_client.pem -outdir . -startdate 101010000000Z -enddate 101011000000Z -extensions v3_ca -cert root.pem -keyfile root.key -infiles expired_client.csr

# Create a Java keystore containing the generated certificate
openssl pkcs12 -export -in expired_client.pem -inkey expired_client.key -out expired_client.p12 -name expired_client -CAfile root.pem -caname druid-it-root -password pass:druid123
keytool -importkeystore -srckeystore expired_client.p12 -srcstoretype PKCS12 -destkeystore expired_client.jks -deststoretype JKS -srcstorepass druid123 -deststorepass druid123
