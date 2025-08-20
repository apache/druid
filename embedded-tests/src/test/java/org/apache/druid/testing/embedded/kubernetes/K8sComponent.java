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

package org.apache.druid.testing.embedded.kubernetes;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Interface for Kubernetes components that can be deployed and managed
 * in a K3S container.
 */
public interface K8sComponent
{
  /**
   * Initialize and deploy the component to the Kubernetes cluster.
   * 
   * @param client the Kubernetes client to use for operations
   * @throws Exception if initialization fails
   */
  void initialize(KubernetesClient client) throws Exception;

  /**
   * Wait for the component to be ready for use.
   * 
   * @param client the Kubernetes client to use for operations
   * @throws Exception if the component fails to become ready
   */
  void waitUntilReady(KubernetesClient client) throws Exception;

  /**
   * Clean up and remove the component from the cluster.
   * 
   * @param client the Kubernetes client to use for operations
   */
  void cleanup(KubernetesClient client);

  /**
   * Get the name of this component for logging purposes.
   * 
   * @return the component name
   */
  String getComponentName();

  /**
   * Get the namespace where this component operates.
   * 
   * @return the namespace name
   */
  String getNamespace();
}
