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

if ($BUILD_DRUID_CLSUTER); then

  DRUID_HOME=$(dirname `pwd`)
  echo "SET DRUID_HOME: $DRUID_HOME"
  minikubeFile="/usr/local/bin/minikube*"
  minikubeFile2="/usr/local/bin/minikube"

  if [ -f "$minikubeFile" ] || [ -f "$minikubeFile2" ]; then
    bash $DRUID_HOME/integration-tests/script/stop_k8s_cluster.sh
  fi

  cd $DRUID_HOME
  echo "Start to setup k8s cluster"
  bash $DRUID_HOME/integration-tests/script/setup_k8s_cluster.sh

  echo "Start to setup druid operator on k8s"
  bash $DRUID_HOME/integration-tests/script/setup_druid_operator_on_k8s.sh

  echo "Start to setup druid on k8s"
  bash $DRUID_HOME/integration-tests/script/setup_druid_on_k8s.sh
fi

