/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.k8s.middlemanager.common;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Pod;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Interface to abstract pod read/update with K8S API Server to allow unit tests with mock impl.
 */
public interface K8sApiClient
{
  V1Pod createPod(String taskID,
                  String image,
                  String namespace,
                  Map<String, String> labels,
                  Map<String, Quantity> resourceLimit,
                  File taskDir,
                  List<String> args,
                  int childPort,
                  int tlsChildPort,
                  String tmpLoc,
                  String peonPodRestartPolicy,
                  String hostPath,
                  String mountPath,
                  String serviceAccountName);
  V1ConfigMap createConfigMap(String namespace, String configmapName, Map<String, String> labels, Map<String, String> data);
  Boolean configMapIsExist(String namespace, String labels);
  void waitForPodRunning(V1Pod peonPod, String labelSelector);
  InputStream getPodLogs(V1Pod peonPod);
  String waitForPodFinished(V1Pod peonPod);
  String getPodStatus(V1Pod peonPod);
  void deletePod(V1Pod peonPod);
  V1Pod getPod(V1Pod peonPod);
  void deleteConfigMap(V1Pod peonPod, String labels);
}
