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

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class K8sDruidLeaderSelectorTest
{
  private final DiscoveryDruidNode testNode1 = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host1", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("druid-cluster", null, null,
                                                                            "default", "default", Duration.millis(10_000), Duration.millis(7_000), Duration.millis(3_000));

  private final String lockResourceName = "druid-leader-election";

  @Test(timeout = 5_000)
  public void testLeaderElection_HappyPath() throws Exception
  {
    K8sDruidLeaderSelector leaderSelector = new K8sDruidLeaderSelector(
        testNode1.getDruidNode(),
        lockResourceName,
        discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace(),
        discoveryConfig,
        new K8sLeaderElectorFactory()
        {
          @Override
          public K8sLeaderElector create(String candidateId, String namespace, String lockResourceName)
          {
            return new K8sLeaderElector()
            {
              @Override
              public String getCurrentLeader()
              {
                return testNode1.getDruidNode().getHostAndPortToUse();
              }

              @Override
              public void run(Runnable startLeadingHook, Runnable stopLeadingHook)
              {
                startLeadingHook.run();
                try {
                  Thread.sleep(Long.MAX_VALUE);
                }
                catch (InterruptedException ex) {
                  stopLeadingHook.run();
                }
              }
            };
          }
        }
    );

    Assert.assertEquals(testNode1.getDruidNode().getHostAndPortToUse(), leaderSelector.getCurrentLeader());

    CountDownLatch becomeLeaderLatch = new CountDownLatch(1);
    CountDownLatch stopBeingLeaderLatch = new CountDownLatch(1);

    leaderSelector.registerListener(
        new DruidLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            becomeLeaderLatch.countDown();
          }

          @Override
          public void stopBeingLeader()
          {
            stopBeingLeaderLatch.countDown();
          }
        }
    );

    becomeLeaderLatch.await();
    leaderSelector.unregisterListener();
    stopBeingLeaderLatch.await();
  }

  @Test(timeout = 5_000)
  public void testLeaderElection_LeaderElectorExits() throws Exception
  {
    K8sDruidLeaderSelector leaderSelector = new K8sDruidLeaderSelector(
        testNode1.getDruidNode(),
        lockResourceName,
        discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace(),
        discoveryConfig,
        new K8sLeaderElectorFactory()
        {
          @Override
          public K8sLeaderElector create(String candidateId, String namespace, String lockResourceName)
          {
            return new K8sLeaderElector()
            {
              private boolean isFirstTime = true;

              @Override
              public String getCurrentLeader()
              {
                return testNode1.getDruidNode().getHostAndPortToUse();
              }

              @Override
              public void run(Runnable startLeadingHook, Runnable stopLeadingHook)
              {
                startLeadingHook.run();

                if (isFirstTime) {
                  isFirstTime = false;
                  stopLeadingHook.run();
                } else {
                  try {
                    Thread.sleep(Long.MAX_VALUE);
                  }
                  catch (InterruptedException ex) {
                    stopLeadingHook.run();
                  }
                }
              }
            };
          }
        }
    );

    Assert.assertEquals(testNode1.getDruidNode().getHostAndPortToUse(), leaderSelector.getCurrentLeader());

    CountDownLatch becomeLeaderLatch = new CountDownLatch(2);
    CountDownLatch stopBeingLeaderLatch = new CountDownLatch(2);

    leaderSelector.registerListener(
        new DruidLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            becomeLeaderLatch.countDown();
          }

          @Override
          public void stopBeingLeader()
          {
            stopBeingLeaderLatch.countDown();
          }
        }
    );

    becomeLeaderLatch.await();
    leaderSelector.unregisterListener();
    stopBeingLeaderLatch.await();
  }
}
