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

# Set Necessary ENV
export CHANGE_MINIKUBE_NONE_USER=true
export MINIKUBE_WANTUPDATENOTIFICATION=false
export MINIKUBE_WANTREPORTERRORPROMPT=false
export MINIKUBE_HOME=$HOME
export KUBECONFIG=$HOME/.kube/config

sudo apt install -y conntrack

# Lacunch K8S cluster
curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v1.18.1/bin/linux/amd64/kubectl && chmod +x kubectl && sudo mv kubectl /usr/local/bin/
curl -Lo minikube https://storage.googleapis.com/minikube/releases/v1.8.1/minikube-linux-amd64 && chmod +x minikube && sudo mv minikube /usr/local/bin/
sudo /usr/local/bin/minikube start --profile=minikube --vm-driver=none --kubernetes-version=v1.18.1
sudo /usr/local/bin/minikube update-context

echo "Setup K8S Cluster Done!"
