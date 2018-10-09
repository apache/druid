#!/bin/bash -eu

./docker/tls/generate-root-certs.sh

mkdir -p client_tls
rm -f client_tls/*
cp docker/tls/root.key client_tls/root.key
cp docker/tls/root.pem client_tls/root.pem
cp docker/tls/untrusted_root.key client_tls/untrusted_root.key
cp docker/tls/untrusted_root.pem client_tls/untrusted_root.pem
cd client_tls

../docker/tls/generate-expired-client-cert.sh
../docker/tls/generate-good-client-cert.sh
../docker/tls/generate-incorrect-hostname-client-cert.sh
../docker/tls/generate-invalid-intermediate-client-cert.sh
../docker/tls/generate-to-be-revoked-client-cert.sh
../docker/tls/generate-untrusted-root-client-cert.sh
../docker/tls/generate-valid-intermediate-client-cert.sh


