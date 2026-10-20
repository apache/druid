#! /bin/bash
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

coordinator_ip=localhost
coordinator_port=8081
protocol=http
authenticator_name=basicAuthenticator

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
