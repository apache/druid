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

export DRUID_OPERATOR_VERSION=0.0.3

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

# Set Necessary ENV
export CHANGE_MINIKUBE_NONE_USER=true
export MINIKUBE_WANTUPDATENOTIFICATION=false
export MINIKUBE_WANTREPORTERRORPROMPT=false
export MINIKUBE_HOME=$HOME
export KUBECONFIG=$HOME/.kube/config
sudo apt install -y conntrack

# This tmp dir is used for MiddleManager pod and Historical Pod to cache segments.
mkdir tmp
chmod 777 tmp

# Lacunch K8S cluster
curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v1.18.1/bin/linux/amd64/kubectl && chmod +x kubectl && sudo mv kubectl /usr/local/bin/
curl -Lo minikube https://storage.googleapis.com/minikube/releases/v1.8.1/minikube-linux-amd64 && chmod +x minikube && sudo mv minikube /usr/local/bin/
sudo /usr/local/bin/minikube start --profile=minikube --vm-driver=none --kubernetes-version=v1.18.1
sudo /usr/local/bin/minikube update-context

# Prepare For Druid-Operator
git clone https://github.com/druid-io/druid-operator.git
cd druid-operator
git checkout -b druid-operator-$DRUID_OPERATOR_VERSION druid-operator-$DRUID_OPERATOR_VERSION
cd ..
sed -i "s|REPLACE_IMAGE|druidio/druid-operator:$DRUID_OPERATOR_VERSION|g" druid-operator/deploy/operator.yaml
cp integration-tests/tiny-cluster.yaml druid-operator/examples/
cp integration-tests/tiny-cluster-zk.yaml druid-operator/examples/
sed -i "s|REPLACE_VOLUMES|`pwd`|g" druid-operator/examples/tiny-cluster.yaml

# Create ZK, Historical, MiddleManager, Overlord-coordiantor, Broker and Router pods using statefulset
sudo /usr/local/bin/kubectl create -f druid-operator/deploy/service_account.yaml
sudo /usr/local/bin/kubectl create -f druid-operator/deploy/role.yaml
sudo /usr/local/bin/kubectl create -f druid-operator/deploy/role_binding.yaml
sudo /usr/local/bin/kubectl create -f druid-operator/deploy/crds/druid.apache.org_druids_crd.yaml
sudo /usr/local/bin/kubectl create -f druid-operator/deploy/operator.yaml
sudo /usr/local/bin/kubectl apply -f druid-operator/examples/tiny-cluster-zk.yaml
sudo /usr/local/bin/kubectl apply -f druid-operator/examples/tiny-cluster.yaml

# Wait 4 * 15 seconds to launch pods.
#count=0
#JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'; until sudo /usr/local/bin/kubectl -n default get pods -lapp=travis-example -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do sleep 4;if [ $count -eq 15 ];then break 2 ;else let "count++";fi;echo $i;echo "waiting for travis-example deployment to be available"; sudo /usr/local/bin/kubectl get pods -n default; done
sleep 120

## Debug And FastFail

sudo /usr/local/bin/kubectl get pod
sudo /usr/local/bin/kubectl get svc
docker images
sudo /usr/local/bin/kubectl describe pod druid-tiny-cluster-middlemanagers-0