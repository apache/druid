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

DOCKER_HOST_IP="$(host "$(hostname)" | perl -nle '/has address (.*)/ && print $1')"
if [ -z "$DOCKER_HOST_IP" ]; then
    # Mac specific way to get host ip
    DOCKER_HOST_IP="$(dscacheutil -q host -a name "$(HOSTNAME)" | perl -nle '/ip_address: (.*)/ && print $1' | tail -n1)"
fi

if [ -z "$DOCKER_HOST_IP" ]; then
    >&2 echo "Could not set docker host IP - integration tests can not run"
    exit 1
fi

export DOCKER_HOST_IP
