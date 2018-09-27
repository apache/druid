#!/bin/bash -eu

export DOCKER_HOST_IP=$(resolveip -s $HOSTNAME)

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
emailAddress=integration-test@druid.io
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
openssl genrsa -out expired_client.key 1024 -sha256
openssl req -new -out expired_client.csr -key expired_client.key -reqexts req_ext -config expired_csr.conf
openssl x509 -req -days -3650 -in expired_client.csr -CA root.pem -CAkey root.key -set_serial 0x11111115 -out expired_client.pem -sha256 -extfile expired_csr.conf -extensions req_ext

# Create a Java keystore containing the generated certificate
openssl pkcs12 -export -in expired_client.pem -inkey expired_client.key -out expired_client.p12 -name expired_client -CAfile root.pem -caname druid-it-root -password pass:druid123
keytool -importkeystore -srckeystore expired_client.p12 -srcstoretype PKCS12 -destkeystore expired_client.jks -deststoretype JKS -srcstorepass druid123 -deststorepass druid123
