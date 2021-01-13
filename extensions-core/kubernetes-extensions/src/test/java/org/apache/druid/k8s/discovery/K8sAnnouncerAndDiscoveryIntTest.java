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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.util.Config;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

/**
 * This is not a UT, but very helpful when making changes to ensure things work with real K8S Api Server.
 * It is ignored in the build but checked in the reporitory for running manually by devs.
 */
@Ignore("Needs K8S API Server")
public class K8sAnnouncerAndDiscoveryIntTest
{
  private final DiscoveryDruidNode testNode = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private final PodInfo podInfo = new PodInfo("postgres-0", "default");

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("druid-cluster", null, null, null, null, null, null, null);

  @Test(timeout = 30000L)
  public void testAnnouncementAndDiscoveryWorkflow() throws Exception
  {
    K8sApiClient k8sApiClient = new DefaultK8sApiClient(Config.defaultClient(), new DefaultObjectMapper());

    K8sDruidNodeDiscoveryProvider discoveryProvider = new K8sDruidNodeDiscoveryProvider(
        podInfo,
        discoveryConfig,
        k8sApiClient
    );
    discoveryProvider.start();

    BooleanSupplier nodeInquirer = discoveryProvider.getForNode(testNode.getDruidNode(), NodeRole.ROUTER);
    Assert.assertFalse(nodeInquirer.getAsBoolean());

    DruidNodeDiscovery discovery = discoveryProvider.getForNodeRole(NodeRole.ROUTER);

    CountDownLatch nodeViewInitialized = new CountDownLatch(1);
    CountDownLatch nodeAppeared = new CountDownLatch(1);
    CountDownLatch nodeDisappeared = new CountDownLatch(1);

    discovery.registerListener(
        new DruidNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
          {
            Iterator<DiscoveryDruidNode> iter = nodes.iterator();
            if (iter.hasNext() && testNode.getDruidNode().getHostAndPort().equals(iter.next().getDruidNode().getHostAndPort())) {
              nodeAppeared.countDown();
            }
          }

          @Override
          public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
          {
            Iterator<DiscoveryDruidNode> iter = nodes.iterator();
            if (iter.hasNext() && testNode.getDruidNode().getHostAndPort().equals(iter.next().getDruidNode().getHostAndPort())) {
              nodeDisappeared.countDown();
            }
          }

          @Override
          public void nodeViewInitialized()
          {
            nodeViewInitialized.countDown();
          }
        }
    );

    nodeViewInitialized.await();

    K8sDruidNodeAnnouncer announcer = new K8sDruidNodeAnnouncer(podInfo, discoveryConfig, k8sApiClient, jsonMapper);
    announcer.announce(testNode);

    nodeAppeared.await();

    Assert.assertTrue(nodeInquirer.getAsBoolean());

    announcer.unannounce(testNode);

    nodeDisappeared.await();

    Assert.assertFalse(nodeInquirer.getAsBoolean());

    discoveryProvider.stop();
  }
}
