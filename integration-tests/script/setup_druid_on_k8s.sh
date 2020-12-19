#!/usr/bin/env bash
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

set -e

export KUBECTL="sudo /usr/local/bin/kubectl"

# setup client keystore
cd integration-tests
./docker/tls/generate-client-certs-and-keystores.sh
rm -rf docker/client_tls
cp -r client_tls docker/client_tls
cd ..

# Build Docker images for pods
mvn -B -ff -q dependency:go-offline \
      install \
      -Pdist,bundle-contrib-exts \
      -Pskip-static-checks,skip-tests \
      -Dmaven.javadoc.skip=true

docker build -t druid/cluster:v1 -f distribution/docker/DockerfileBuildTarAdvanced .

# This tmp dir is used for MiddleManager pod and Historical Pod to cache segments.
mkdir tmp
chmod 777 tmp

$KUBECTL apply -f integration-tests/k8s/role-and-binding.yaml
sed -i "s|REPLACE_VOLUMES|`pwd`|g" integration-tests/k8s/tiny-cluster.yaml
$KUBECTL apply -f integration-tests/k8s/tiny-cluster.yaml

# Wait 4 * 15 seconds to launch pods.
#count=0
#JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'; until sudo /usr/local/bin/kubectl -n default get pods -lapp=travis-example -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do sleep 4;if [ $count -eq 15 ];then break 2 ;else let "count++";fi;echo $i;echo "waiting for travis-example deployment to be available"; sudo /usr/local/bin/kubectl get pods -n default; done
sleep 120

## Debug And FastFail

sudo /usr/local/bin/kubectl get pod
sudo /usr/local/bin/kubectl get svc

