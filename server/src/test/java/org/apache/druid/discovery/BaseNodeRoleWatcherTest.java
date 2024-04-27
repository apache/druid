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

package org.apache.druid.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.DruidNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseNodeRoleWatcherTest
{
  private static ScheduledExecutorService exec;

  @BeforeClass
  public static void setup()
  {
    exec = createScheduledSingleThreadedExecutor();
  }

  @AfterClass
  public static void teardown()
  {
    exec.shutdown();
  }

  @Test(timeout = 60_000L)
  public void testGeneralUseSimulation()
  {
    BaseNodeRoleWatcher nodeRoleWatcher = BaseNodeRoleWatcher.create(exec, NodeRole.BROKER);

    DiscoveryDruidNode broker1 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker1");
    DiscoveryDruidNode broker2 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker2");
    DiscoveryDruidNode broker3 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker3");

    DiscoveryDruidNode notBroker = new DiscoveryDruidNode(
        new DruidNode("s3", "h3", false, 8080, null, true, false),
        NodeRole.COORDINATOR,
        ImmutableMap.of()
    );

    TestListener listener1 = new TestListener();
    TestListener listener2 = new TestListener();
    TestListener listener3 = new TestListener();

    nodeRoleWatcher.registerListener(listener1);
    nodeRoleWatcher.childAdded(broker1);
    nodeRoleWatcher.childAdded(broker2);
    nodeRoleWatcher.childAdded(notBroker);
    nodeRoleWatcher.childAdded(broker3);
    nodeRoleWatcher.registerListener(listener2);
    nodeRoleWatcher.childRemoved(broker2);

    assertListener(listener1, false, Collections.emptyList(), Collections.emptyList());
    assertListener(listener2, false, Collections.emptyList(), Collections.emptyList());

    nodeRoleWatcher.cacheInitialized();

    nodeRoleWatcher.registerListener(listener3);

    List<DiscoveryDruidNode> presentNodes = new ArrayList<>(nodeRoleWatcher.getAllNodes());
    Assert.assertEquals(2, presentNodes.size());
    Assert.assertTrue(presentNodes.contains(broker1));
    Assert.assertTrue(presentNodes.contains(broker3));

    assertListener(listener1, true, presentNodes, Collections.emptyList());
    assertListener(listener2, true, presentNodes, Collections.emptyList());
    assertListener(listener3, true, presentNodes, Collections.emptyList());

    nodeRoleWatcher.childRemoved(notBroker);
    nodeRoleWatcher.childRemoved(broker2);
    nodeRoleWatcher.childAdded(broker2);
    nodeRoleWatcher.childRemoved(broker3);
    nodeRoleWatcher.childAdded(broker1);

    Assert.assertEquals(ImmutableSet.of(broker2, broker1), new HashSet<>(nodeRoleWatcher.getAllNodes()));

    List<DiscoveryDruidNode> nodesAdded = new ArrayList<>(presentNodes);
    nodesAdded.add(broker2);

    List<DiscoveryDruidNode> nodesRemoved = new ArrayList<>();
    nodesRemoved.add(broker3);

    assertListener(listener1, true, nodesAdded, nodesRemoved);
    assertListener(listener2, true, nodesAdded, nodesRemoved);
    assertListener(listener3, true, nodesAdded, nodesRemoved);

    LinkedHashMap<String, DiscoveryDruidNode> resetNodes = new LinkedHashMap<>();
    resetNodes.put(broker2.getDruidNode().getHostAndPortToUse(), broker2);
    resetNodes.put(broker3.getDruidNode().getHostAndPortToUse(), broker3);

    nodeRoleWatcher.resetNodes(resetNodes);

    Assert.assertEquals(ImmutableSet.of(broker2, broker3), new HashSet<>(nodeRoleWatcher.getAllNodes()));

    nodesAdded.add(broker3);
    nodesRemoved.add(broker1);

    assertListener(listener1, true, nodesAdded, nodesRemoved);
    assertListener(listener2, true, nodesAdded, nodesRemoved);
    assertListener(listener3, true, nodesAdded, nodesRemoved);
  }

  @Test(timeout = 60_000L)
  public void testRegisterListenerBeforeTimeout() throws InterruptedException
  {
    BaseNodeRoleWatcher nodeRoleWatcher = new BaseNodeRoleWatcher(exec, NodeRole.BROKER);

    TestListener listener1 = new TestListener();
    nodeRoleWatcher.registerListener(listener1);

    DiscoveryDruidNode broker1 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker1");
    DiscoveryDruidNode broker2 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker2");
    DiscoveryDruidNode broker3 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker3");

    DiscoveryDruidNode notBroker = new DiscoveryDruidNode(
        new DruidNode("s3", "h3", false, 8080, null, true, false),
        NodeRole.COORDINATOR,
        ImmutableMap.of()
    );

    nodeRoleWatcher.childAdded(broker1);
    nodeRoleWatcher.childAdded(notBroker);
    nodeRoleWatcher.childAdded(broker3);
    nodeRoleWatcher.childRemoved(broker2);

    assertListener(listener1, false, Collections.emptyList(), Collections.emptyList());

    nodeRoleWatcher.scheduleTimeout(0);
    nodeRoleWatcher.awaitInitialization();

    Assert.assertTrue(listener1.nodeViewInitializationTimedOut.get());

    assertListener(listener1, true, ImmutableList.of(broker1, broker3), ImmutableList.of());
  }

  @Test(timeout = 60_000L)
  public void testGetAllNodesBeforeTimeout() throws InterruptedException
  {
    BaseNodeRoleWatcher nodeRoleWatcher = new BaseNodeRoleWatcher(exec, NodeRole.BROKER);

    TestListener listener1 = new TestListener();
    nodeRoleWatcher.registerListener(listener1);

    DiscoveryDruidNode broker1 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker1");
    DiscoveryDruidNode broker2 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker2");
    DiscoveryDruidNode broker3 = buildDiscoveryDruidNode(NodeRole.BROKER, "broker3");

    DiscoveryDruidNode notBroker = new DiscoveryDruidNode(
        new DruidNode("s3", "h3", false, 8080, null, true, false),
        NodeRole.COORDINATOR,
        ImmutableMap.of()
    );

    nodeRoleWatcher.childAdded(broker1);
    nodeRoleWatcher.childAdded(broker2);
    nodeRoleWatcher.childAdded(notBroker);
    nodeRoleWatcher.childAdded(broker3);
    nodeRoleWatcher.childRemoved(broker2);

    assertListener(listener1, false, Collections.emptyList(), Collections.emptyList());

    nodeRoleWatcher.scheduleTimeout(0);
    nodeRoleWatcher.awaitInitialization();

    Assert.assertEquals(2, nodeRoleWatcher.getAllNodes().size());

    Assert.assertTrue(listener1.nodeViewInitializationTimedOut.get());
    assertListener(listener1, true, ImmutableList.of(broker1, broker3), ImmutableList.of());
  }

  private DiscoveryDruidNode buildDiscoveryDruidNode(NodeRole role, String host)
  {
    return new DiscoveryDruidNode(
        new DruidNode("s", host, false, 8080, null, true, false),
        role,
        ImmutableMap.of()
    );
  }

  private void assertListener(
      TestListener listener,
      boolean ready,
      List<DiscoveryDruidNode> nodesAdded,
      List<DiscoveryDruidNode> nodesRemoved
  )
  {
    final int count = ready ? 0 : 1;
    Assert.assertEquals(count, listener.ready.getCount());
    Assert.assertEquals(nodesAdded, listener.nodesAddedList);
    Assert.assertEquals(nodesRemoved, listener.nodesRemovedList);
  }

  private static ScheduledExecutorService createScheduledSingleThreadedExecutor()
  {
    return new ScheduledThreadPoolExecutor(
        1,
        Execs.makeThreadFactory("BaseNodeRoleWatcher")
    )
    {
      @Override
      public Future<?> submit(Runnable task)
      {
        task.run();
        return Futures.immediateFuture(null);
      }
    };
  }

  public static class TestListener implements DruidNodeDiscovery.Listener
  {
    private final CountDownLatch ready = new CountDownLatch(1);
    private final AtomicBoolean nodeViewInitializationTimedOut = new AtomicBoolean(false);
    private final List<DiscoveryDruidNode> nodesAddedList = new ArrayList<>();
    private final List<DiscoveryDruidNode> nodesRemovedList = new ArrayList<>();

    @Override
    public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
    {
      nodesAddedList.addAll(nodes);
    }

    @Override
    public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
    {
      nodesRemovedList.addAll(nodes);
    }

    @Override
    public void nodeViewInitialized()
    {
      if (ready.getCount() == 0) {
        throw new RuntimeException("NodeViewInitialized called again!");
      }
      ready.countDown();
    }

    @Override
    public void nodeViewInitializedTimedOut()
    {
      if (!nodeViewInitializationTimedOut.compareAndSet(false, true)) {
        throw new RuntimeException("NodeViewInitializedTimedOut called again!");
      } else if (ready.getCount() == 0) {
        throw new RuntimeException("NodeViewInitialized was already called!");
      }
      ready.countDown();
    }
  }
}
