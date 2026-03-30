#!/bin/bash
# Get the directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Check if the bootstrap.ldif file exists on the host
if [ ! -f "$DIR/bootstrap.ldif" ]; then
    echo "Error: $DIR/bootstrap.ldif not found."
    exit 1
fi

# Stop and remove any existing ldap-mock container
docker rm -f ldap-mock 2>/dev/null || true

# Start the ldap-mock container with correct mount
docker run -d \
  --name ldap-mock \
  -p 8389:389 \
  -v "$DIR/bootstrap.ldif:/container/service/slapd/assets/config/bootstrap/ldif/custom/bootstrap.ldif" \
  -e LDAP_DOMAIN="example.org" \
  -e LDAP_ORGANISATION="Example" \
  -e LDAP_ADMIN_PASSWORD="admin" \
  osixia/openldap:1.5.0 --copy-service
