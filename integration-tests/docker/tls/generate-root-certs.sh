#!/bin/bash -eu

rm -f root.key
rm -f untrusted_root.key
rm -f root.pem
rm -f untrusted_root.pem

openssl genrsa -out docker/tls/root.key 4096
openssl genrsa -out docker/tls/untrusted_root.key 4096

openssl req -config docker/tls/root.cnf -key docker/tls/root.key -new -x509 -days 3650 -sha256 -extensions v3_ca -out docker/tls/root.pem
openssl req -config docker/tls/root.cnf -key docker/tls/untrusted_root.key -new -x509 -days 3650 -sha256 -extensions v3_ca -out docker/tls/untrusted_root.pem

