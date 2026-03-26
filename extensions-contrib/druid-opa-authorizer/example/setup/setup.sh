#! /bin/bash

coordinator_ip=localhost
coordinator_port=8081
protocol=http
authenticator_name=MyBasicMetadataAuthenticator

create_user() {
    local user=$1
    curl -u admin:password1 -XPOST ${protocol}://$coordinator_ip:${coordinator_port}/druid-ext/basic-security/authentication/db/${authenticator_name}/users/${user}
    curl -u admin:password1 -H'Content-Type: application/json' -XPOST --data-binary @${user}_pass.json ${protocol}://$coordinator_ip:${coordinator_port}/druid-ext/basic-security/authentication/db/${authenticator_name}/users/${user}/credentials
}

create_user alice
create_user bob
create_user christy
create_user dylan
create_user eve
