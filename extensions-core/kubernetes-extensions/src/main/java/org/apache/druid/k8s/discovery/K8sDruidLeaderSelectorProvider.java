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
import com.google.inject.Provider;
import io.kubernetes.client.openapi.ApiClient;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DruidNode;

public abstract class K8sDruidLeaderSelectorProvider implements Provider<DruidLeaderSelector>
{
  @Inject
  @Self
  private DruidNode druidNode;

  @Inject
  private PodInfo podInfo;

  @Inject
  private K8sDiscoveryConfig discoveryConfig;

  @Inject
  private Provider<ApiClient> k8sApiClientProvider;

  private boolean isCoordinator;

  K8sDruidLeaderSelectorProvider(boolean isCoordinator)
  {
    this.isCoordinator = isCoordinator;
  }

  @Override
  public DruidLeaderSelector get()
  {
    // Note: these can not be setup in the constructor because injected K8sDiscoveryConfig and PodInfo
    // are not available at that time.
    String lockResourceName;
    String lockResourceNamespace;

    if (isCoordinator) {
      lockResourceName = discoveryConfig.getClusterIdentifier() + "-leaderelection-coordinator";
      lockResourceNamespace = discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace() == null
                              ?
                              podInfo.getPodNamespace()
                              : discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace();
    } else {
      lockResourceName = discoveryConfig.getClusterIdentifier() + "-leaderelection-overlord";
      lockResourceNamespace = discoveryConfig.getOverlordLeaderElectionConfigMapNamespace() == null ?
                              podInfo.getPodNamespace() : discoveryConfig.getOverlordLeaderElectionConfigMapNamespace();
    }

    return new K8sDruidLeaderSelector(
        druidNode,
        lockResourceName,
        lockResourceNamespace,
        discoveryConfig,
        new DefaultK8sLeaderElectorFactory(k8sApiClientProvider.get(), discoveryConfig)
    );
  }

  static class K8sCoordinatorDruidLeaderSelectorProvider extends K8sDruidLeaderSelectorProvider
  {
    @Inject
    public K8sCoordinatorDruidLeaderSelectorProvider()
    {
      super(true);
    }
  }

  static class K8sIndexingServiceDruidLeaderSelectorProvider extends K8sDruidLeaderSelectorProvider
  {
    @Inject
    public K8sIndexingServiceDruidLeaderSelectorProvider()
    {
      super(false);
    }
  }
}
