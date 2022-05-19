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
import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.extended.leaderelection.Lock;
import io.kubernetes.client.extended.leaderelection.resourcelock.ConfigMapLock;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import org.apache.druid.java.util.common.RE;

import java.time.Duration;

/**
 * Concrete {@link K8sLeaderElectorFactory} impl using k8s-client java lib.
 */
public class DefaultK8sLeaderElectorFactory implements K8sLeaderElectorFactory
{
  private final ApiClient realK8sClient;
  private final K8sDiscoveryConfig discoveryConfig;

  @Inject
  public DefaultK8sLeaderElectorFactory(ApiClient realK8sClient, K8sDiscoveryConfig discoveryConfig)
  {
    this.realK8sClient = realK8sClient;
    this.discoveryConfig = discoveryConfig;
  }

  @Override
  public K8sLeaderElector create(String candidateId, String namespace, String lockResourceName)
  {
    Lock lock = createLock(candidateId, namespace, lockResourceName, realK8sClient);
    LeaderElectionConfig leaderElectionConfig =
        new LeaderElectionConfig(
            lock,
            Duration.ofMillis(discoveryConfig.getLeaseDuration().getMillis()),
            Duration.ofMillis(discoveryConfig.getRenewDeadline().getMillis()),
            Duration.ofMillis(discoveryConfig.getRetryPeriod().getMillis())
        );
    LeaderElector leaderElector = new LeaderElector(leaderElectionConfig);

    return new K8sLeaderElector()
    {
      @Override
      public String getCurrentLeader()
      {
        try {
          return lock.get().getHolderIdentity();
        }
        catch (ApiException ex) {
          throw new RE(ex, "Failed  to get current leader for [%s]", lockResourceName);
        }
      }

      @Override
      public void run(Runnable startLeadingHook, Runnable stopLeadingHook)
      {
        leaderElector.run(startLeadingHook, stopLeadingHook);
      }
    };
  }

  private Lock createLock(String candidateId, String namespace, String lockResourceName, ApiClient k8sApiClient)
  {
    return new ConfigMapLock(
        namespace,
        lockResourceName,
        candidateId,
        k8sApiClient
    );
  }
}
