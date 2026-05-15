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

package org.apache.druid.consul.discovery;

import com.google.common.collect.ImmutableList;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsulDruidNodeDiscoveryProviderTest
{
  private final DiscoveryDruidNode node1 = new DiscoveryDruidNode(
      new DruidNode("druid/broker", "host1", true, 8082, null, true, false),
      NodeRole.BROKER,
      null
  );

  private final DiscoveryDruidNode node2 = new DiscoveryDruidNode(
      new DruidNode("druid/broker", "host2", true, 8082, null, true, false),
      NodeRole.BROKER,
      null
  );

  private final ConsulDiscoveryConfig config = TestUtils.builder()
      .servicePrefix("druid")
      .healthCheckInterval(Duration.millis(10000))
      .deregisterAfter(Duration.millis(90000))
      .watchSeconds(Duration.millis(60000))
      .watchRetryDelay(Duration.millis(10000))
      .build();

  private ConsulApiClient mockConsulApiClient;
  private ConsulDruidNodeDiscoveryProvider provider;

  @Before
  public void setUp()
  {
    mockConsulApiClient = EasyMock.createMock(ConsulApiClient.class);
    provider = new ConsulDruidNodeDiscoveryProvider(mockConsulApiClient, config);
  }

  @After
  public void tearDown()
  {
    if (provider != null) {
      provider.stop();
    }
  }

  @Test
  public void testGetForNode() throws Exception
  {
    List<DiscoveryDruidNode> nodes = ImmutableList.of(node1, node2);

    EasyMock.expect(mockConsulApiClient.getHealthyServices(NodeRole.BROKER))
            .andReturn(nodes)
            .once();

    EasyMock.replay(mockConsulApiClient);

    provider.start();

    boolean found = provider.getForNode(node1.getDruidNode(), NodeRole.BROKER).getAsBoolean();
    Assert.assertTrue(found);

    EasyMock.verify(mockConsulApiClient);
  }

  @Test
  public void testGetForNodeRole() throws Exception
  {
    List<DiscoveryDruidNode> initialNodes = ImmutableList.of(node1);

    EasyMock.expect(mockConsulApiClient.getHealthyServices(NodeRole.BROKER))
            .andReturn(initialNodes)
            .once();

    // First watch call with index 0
    EasyMock.expect(mockConsulApiClient.watchServices(
        EasyMock.eq(NodeRole.BROKER),
        EasyMock.eq(0L),
        EasyMock.anyLong()
    ))
            .andReturn(new ConsulApiClient.ConsulWatchResult(initialNodes, 1L))
            .once();

    // Subsequent watch calls with index 1 (the response index from first call)
    EasyMock.expect(mockConsulApiClient.watchServices(
        EasyMock.eq(NodeRole.BROKER),
        EasyMock.eq(1L),
        EasyMock.anyLong()
    ))
            .andReturn(new ConsulApiClient.ConsulWatchResult(initialNodes, 1L))
            .anyTimes();

    EasyMock.replay(mockConsulApiClient);

    provider.start();

    DruidNodeDiscovery discovery = provider.getForNodeRole(NodeRole.BROKER);
    Assert.assertNotNull(discovery);

    // Wait a bit for cache to initialize
    Thread.sleep(500);

    Collection<DiscoveryDruidNode> nodes = discovery.getAllNodes();
    Assert.assertEquals(1, nodes.size());
    Assert.assertTrue(nodes.contains(node1));

    EasyMock.verify(mockConsulApiClient);
  }

  @Test
  public void testListenerNotifications() throws Exception
  {
    List<DiscoveryDruidNode> initialNodes = ImmutableList.of(node1);
    List<DiscoveryDruidNode> updatedNodes = ImmutableList.of(node1, node2);

    CountDownLatch initLatch = new CountDownLatch(1);
    CountDownLatch addedLatch = new CountDownLatch(1);

    EasyMock.expect(mockConsulApiClient.getHealthyServices(NodeRole.BROKER))
            .andReturn(initialNodes)
            .once();

    // First watch returns no changes
    EasyMock.expect(mockConsulApiClient.watchServices(
        EasyMock.eq(NodeRole.BROKER),
        EasyMock.eq(0L),
        EasyMock.anyLong()
    ))
            .andReturn(new ConsulApiClient.ConsulWatchResult(initialNodes, 0L))
            .once();

    // Second watch returns updated nodes
    EasyMock.expect(mockConsulApiClient.watchServices(
        EasyMock.eq(NodeRole.BROKER),
        EasyMock.eq(0L),
        EasyMock.anyLong()
    ))
            .andReturn(new ConsulApiClient.ConsulWatchResult(updatedNodes, 1L))
            .once();

    // Continue returning updated nodes
    EasyMock.expect(mockConsulApiClient.watchServices(
        EasyMock.eq(NodeRole.BROKER),
        EasyMock.eq(1L),
        EasyMock.anyLong()
    ))
            .andReturn(new ConsulApiClient.ConsulWatchResult(updatedNodes, 1L))
            .anyTimes();

    EasyMock.replay(mockConsulApiClient);

    provider.start();

    DruidNodeDiscovery discovery = provider.getForNodeRole(NodeRole.BROKER);

    discovery.registerListener(new DruidNodeDiscovery.Listener()
    {
      @Override
      public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
      {
        if (nodes.contains(node2)) {
          addedLatch.countDown();
        }
      }

      @Override
      public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
      {
        // Not expected
      }

      @Override
      public void nodeViewInitialized()
      {
        initLatch.countDown();
      }
    });

    Assert.assertTrue("Initialization timed out", initLatch.await(5, TimeUnit.SECONDS));
    Assert.assertTrue("Node addition not detected", addedLatch.await(5, TimeUnit.SECONDS));

    EasyMock.verify(mockConsulApiClient);
  }
}
