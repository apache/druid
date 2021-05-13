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

export KUBECTL="/usr/local/bin/kubectl"

DRUID_CLUSTER_SPEC_YAML=$1

echo "Druid Cluster Spec ${DRUID_CLUSTER_SPEC_YAML}"

# setup client keystore
cd integration-tests
./docker/tls/generate-client-certs-and-keystores.sh
rm -rf docker/client_tls
cp -r client_tls docker/client_tls
cd ..

# Build Docker images for pods
#mvn -B -ff -q dependency:go-offline \
#      install \
#      -Pdist,bundle-contrib-exts \
#      -Pskip-static-checks,skip-tests \
#      -Dmaven.javadoc.skip=true

docker build --build-arg BUILD_FROM_SOURCE=0 -t druid/base:v1 -f distribution/docker/Dockerfile .
docker build --build-arg BASE_IMAGE=druid/base:v1 -t druid/cluster:v1 -f distribution/docker/DockerfileBuildTarAdvanced .

# This tmp dir is used for MiddleManager pod and Historical Pod to cache segments.
sudo rm -rf tmp
mkdir tmp
chmod 777 tmp

# Technically postgres isn't needed in all tests i.e. where derby is used, but ok.
$KUBECTL apply -f integration-tests/k8s/postgres.yaml

# spec name needs to come from argument for high availability test
$KUBECTL apply -f integration-tests/k8s/role-and-binding.yaml
sed -i.bak "s|REPLACE_VOLUMES|`pwd`|g" ${DRUID_CLUSTER_SPEC_YAML}
$KUBECTL apply -f ${DRUID_CLUSTER_SPEC_YAML}

# Wait a bit
#sleep 60

## Debug And FastFail

$KUBECTL get pod
$KUBECTL get svc

for v in druid-tiny-cluster-coordinator1-0 druid-tiny-cluster-coordinator2-0 druid-tiny-cluster-overlord1-0 druid-tiny-cluster-overlord2-0 ; do
  $KUBECTL logs --tail 1000 $v
done