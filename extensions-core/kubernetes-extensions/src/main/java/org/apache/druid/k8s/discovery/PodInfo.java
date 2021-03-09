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

package org.apache.druid.k8s.discovery;

import com.google.inject.Inject;
import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.guice.LazySingleton;

@LazySingleton
public class PodInfo
{
  private final String podName;
  private final String podNamespace;

  @Inject
  public PodInfo(K8sDiscoveryConfig discoveryConfig)
  {
    this.podName = System.getenv(discoveryConfig.getPodNameEnvKey());
    Preconditions.checkState(podName != null && !podName.isEmpty(), "Failed to find podName");

    this.podNamespace = System.getenv(discoveryConfig.getPodNamespaceEnvKey());
    Preconditions.checkState(podNamespace != null && !podNamespace.isEmpty(), "Failed to find podNamespace");
  }

  @VisibleForTesting
  public PodInfo(String podName, String podNamespace)
  {
    this.podName = podName;
    this.podNamespace = podNamespace;
  }

  public String getPodName()
  {
    return podName;
  }

  public String getPodNamespace()
  {
    return podNamespace;
  }
}
