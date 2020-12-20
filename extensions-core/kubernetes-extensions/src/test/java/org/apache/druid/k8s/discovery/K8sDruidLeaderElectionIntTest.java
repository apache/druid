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

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.util.Config;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is not a UT, but very helpful when making changes to ensure things work with real K8S Api Server.
 * It is ignored in the build but checked in the reporitory for running manually by devs.
 */
@Ignore("Needs K8S API Server")
public class K8sDruidLeaderElectionIntTest
{
  private static final Logger LOGGER = new Logger(K8sDruidLeaderElectionIntTest.class);

  private final DiscoveryDruidNode testNode1 = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host1", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final DiscoveryDruidNode testNode2 = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host2", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("druid-cluster", null, null, "default", "default",
                                                                            null, null, null);

  private final ApiClient k8sApiClient;

  private final String lockResourceName = "druid-leader-election";

  public K8sDruidLeaderElectionIntTest() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    k8sApiClient = Config.defaultClient();
  }

  // Note: This one is supposed to crash.
  @Test(timeout = 60000L)
  public void test_becomeLeader_exception() throws Exception
  {
    K8sDruidLeaderSelector leaderSelector = new K8sDruidLeaderSelector(testNode1.getDruidNode(), lockResourceName, discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace(), discoveryConfig, new DefaultK8sLeaderElectorFactory(k8sApiClient, discoveryConfig));

    CountDownLatch becomeLeaderLatch = new CountDownLatch(1);
    CountDownLatch stopBeingLeaderLatch = new CountDownLatch(1);

    AtomicBoolean failed = new AtomicBoolean(false);

    leaderSelector.registerListener(new DruidLeaderSelector.Listener()
    {
      @Override
      public void becomeLeader()
      {
        becomeLeaderLatch.countDown();
        // This leads to a System.exit() and pod restart is expected to happen.
        throw new RuntimeException("Leader crashed");
      }

      @Override
      public void stopBeingLeader()
      {
        try {
          // wait to make sure start-being-leader notification came first
          becomeLeaderLatch.await();
          stopBeingLeaderLatch.countDown();
        }
        catch (InterruptedException ex) {
          failed.set(true);
        }
      }
    });

    LOGGER.info("Waiting for leadership notification...");
    becomeLeaderLatch.await();

    LOGGER.info("Waiting for stop-being-leader notification...");
    stopBeingLeaderLatch.await();

    Assert.assertFalse(failed.get());
  }

  @Test(timeout = 60000L)
  public void test_leaderCandidate_stopped() throws Exception
  {
    // delete the lock resource if it exists, or else first leader candidate would need to wait for a whole
    // leaseDuration configured
    try {
      CoreV1Api coreV1Api = new CoreV1Api(k8sApiClient);
      coreV1Api.deleteNamespacedConfigMap(
          lockResourceName,
          discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace(),
          null,
          null,
          null,
          null,
          null,
          null
      );
      LOGGER.info("Deleted existing lock resource [%s]", lockResourceName);
    }
    catch (ApiException ex) {
      if (ex.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw ex;
      }
    }

    K8sDruidLeaderSelector leaderSelector1 = new K8sDruidLeaderSelector(testNode1.getDruidNode(), lockResourceName, discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace(), discoveryConfig, new DefaultK8sLeaderElectorFactory(k8sApiClient, discoveryConfig));

    CountDownLatch becomeLeaderLatch1 = new CountDownLatch(1);
    CountDownLatch stopBeingLeaderLatch1 = new CountDownLatch(1);

    AtomicBoolean failed1 = new AtomicBoolean(false);

    leaderSelector1.registerListener(new DruidLeaderSelector.Listener()
    {
      @Override
      public void becomeLeader()
      {
        becomeLeaderLatch1.countDown();
      }

      @Override
      public void stopBeingLeader()
      {
        try {
          // wait to make sure start-being-leader notification came first
          becomeLeaderLatch1.await();
          stopBeingLeaderLatch1.countDown();
        }
        catch (InterruptedException ex) {
          failed1.set(true);
        }
      }
    });

    LOGGER.info("Waiting for candidate#1 start-being-leader notification...");
    becomeLeaderLatch1.await();
    LOGGER.info("Candidate#1 start-being-leader notification arrived.");

    K8sDruidLeaderSelector leaderSelector2 = new K8sDruidLeaderSelector(testNode2.getDruidNode(), lockResourceName, discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace(), discoveryConfig, new DefaultK8sLeaderElectorFactory(k8sApiClient, discoveryConfig));
    CountDownLatch becomeLeaderLatch2 = new CountDownLatch(1);
    leaderSelector2.registerListener(new DruidLeaderSelector.Listener()
    {
      @Override
      public void becomeLeader()
      {
        becomeLeaderLatch2.countDown();
      }

      @Override
      public void stopBeingLeader()
      {
      }
    });

    LOGGER.info("Waiting for candidate#1 start-being-leader notification...");
    becomeLeaderLatch1.await();
    LOGGER.info("Candidate#1 start-being-leader notification arrived.");

    LOGGER.info("Candidate#1 stopping leader election...");
    leaderSelector1.unregisterListener();

    LOGGER.info("Waiting for candidate#1 to receive stop-being-leader notification.");
    stopBeingLeaderLatch1.await();
    Assert.assertFalse(failed1.get());

    LOGGER.info("Waiting for candidate#2 start-being-leader notification...");
    becomeLeaderLatch2.await();
  }
}
