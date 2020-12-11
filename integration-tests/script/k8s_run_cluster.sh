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
if [ `whoami` != "root" ];
  then
    echo "Need root when build K8S cluster !"
    exit -1
fi
# Setup minikube, kubectl and druid-operator

# Export all the ENV needed
export CHANGE_MINIKUBE_NONE_USER=true
export MINIKUBE_WANTUPDATENOTIFICATION=false
export MINIKUBE_WANTREPORTERRORPROMPT=false
export MINIKUBE_HOME=$HOME
export KUBECONFIG=$HOME/.kube/config

# Setup minikube and kubectl
apt-get -qq -y install conntrack
curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v1.18.1/bin/linux/amd64/kubectl && chmod +x kubectl && mv kubectl /usr/local/bin/
curl -Lo minikube https://storage.googleapis.com/minikube/releases/v1.8.1/minikube-linux-amd64 && chmod +x minikube && mv minikube /usr/local/bin/
mkdir -p $HOME/.kube $HOME/.minikube
touch $KUBECONFIG
/usr/local/bin/minikube start --profile=minikube --vm-driver=none --kubernetes-version=v1.18.1

# Setup druid-operator
git clone https://github.com/druid-io/druid-operator.git
docker pull druidio/druid-operator:0.0.3
sed -i 's|REPLACE_IMAGE|druidio/druid-operator:0.0.3|g' druid-operator/deploy/operator.yaml
cp tiny-cluster.yaml druid-operator/examples/

# Create Everythings using POD
/usr/local/bin/kubectl create -f deploy/service_account.yaml
/usr/local/bin/kubectl create -f deploy/role.yaml
/usr/local/bin/kubectl create -f deploy/role_binding.yaml
/usr/local/bin/kubectl create -f deploy/crds/druid.apache.org_druids_crd.yaml
/usr/local/bin/kubectl create -f deploy/operator.yaml
/usr/local/bin/kubectl apply -f examples/tiny-cluster-zk.yaml
/usr/local/bin/kubectl apply -f examples/tiny-cluster.yaml

# Wait 2 * 15 seconds to launch pods.
count=0
JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'; until kubectl -n default get pods -lapp=travis-example -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do sleep 2;if [ $count -eq 15 ];then break 2 ;else let "count++";fi;echo $i;echo "waiting for travis-example deployment to be available"; kubectl get pods -n default; done

