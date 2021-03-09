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

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.com.google.common.collect.Lists;
import io.kubernetes.client.util.Watch;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class K8sDruidNodeDiscoveryProviderTest
{
  private static final Logger LOGGER = new Logger(K8sDruidNodeDiscoveryProviderTest.class);

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

  private final DiscoveryDruidNode testNode3 = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host3", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final DiscoveryDruidNode testNode4 = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host4", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final DiscoveryDruidNode testNode5 = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host5", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final PodInfo podInfo = new PodInfo("testpod", "testns");

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("druid-cluster", null, null, null, null, null, null, null);

  @Test(timeout = 60_000)
  public void testGetForNodeRole() throws Exception
  {
    String labelSelector = "druidDiscoveryAnnouncement-cluster-identifier=druid-cluster,druidDiscoveryAnnouncement-router=true";
    K8sApiClient mockK8sApiClient = EasyMock.createMock(K8sApiClient.class);
    EasyMock.expect(mockK8sApiClient.listPods(podInfo.getPodNamespace(), labelSelector, NodeRole.ROUTER)).andReturn(
        new DiscoveryDruidNodeList(
            "v1",
            ImmutableMap.of(
                testNode1.getDruidNode().getHostAndPortToUse(), testNode1,
                testNode2.getDruidNode().getHostAndPortToUse(), testNode2
            )
        )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v1", NodeRole.ROUTER)).andReturn(null);
    EasyMock.expect(mockK8sApiClient.listPods(podInfo.getPodNamespace(), labelSelector, NodeRole.ROUTER)).andReturn(
        new DiscoveryDruidNodeList(
            "v2",
            ImmutableMap.of(
                testNode2.getDruidNode().getHostAndPortToUse(), testNode2,
                testNode3.getDruidNode().getHostAndPortToUse(), testNode3
            )
        )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v2", NodeRole.ROUTER)).andReturn(
            new MockWatchResult(Collections.emptyList(), true, false)
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v2", NodeRole.ROUTER)).andReturn(
        new MockWatchResult(
            ImmutableList.of(
                  new Watch.Response<>(WatchResult.ADDED, new DiscoveryDruidNodeAndResourceVersion("v3", testNode4)),
                  new Watch.Response<>(WatchResult.DELETED, new DiscoveryDruidNodeAndResourceVersion("v4", testNode2))
              ),
            false,
            true
            )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v4", NodeRole.ROUTER)).andReturn(
        new MockWatchResult(
            ImmutableList.of(
                new Watch.Response<>(WatchResult.ADDED, new DiscoveryDruidNodeAndResourceVersion("v5", testNode5)),
                new Watch.Response<>(WatchResult.DELETED, new DiscoveryDruidNodeAndResourceVersion("v6", testNode3))
            ),
            false,
            false
        )
    );
    EasyMock.replay(mockK8sApiClient);

    K8sDruidNodeDiscoveryProvider discoveryProvider = new K8sDruidNodeDiscoveryProvider(
        podInfo,
        discoveryConfig,
        mockK8sApiClient,
        1
    );
    discoveryProvider.start();

    K8sDruidNodeDiscoveryProvider.NodeRoleWatcher nodeDiscovery = discoveryProvider.getForNodeRole(NodeRole.ROUTER, false);

    MockListener testListener = new MockListener(
        ImmutableList.of(
            MockListener.Event.added(testNode1),
            MockListener.Event.added(testNode2),
            MockListener.Event.inited(),
            MockListener.Event.added(testNode3),
            MockListener.Event.deleted(testNode1),
            MockListener.Event.added(testNode4),
            MockListener.Event.deleted(testNode2),
            MockListener.Event.added(testNode5),
            MockListener.Event.deleted(testNode3)
        )
    );
    nodeDiscovery.registerListener(testListener);

    nodeDiscovery.start();

    testListener.assertSuccess();

    discoveryProvider.stop();
  }

  private static class MockListener implements DruidNodeDiscovery.Listener
  {
    List<Event> events;
    private boolean failed = false;
    private String failReason;

    public MockListener(List<Event> events)
    {
      this.events = Lists.newArrayList(events);
    }

    @Override
    public void nodeViewInitialized()
    {
      assertNextEvent(Event.inited());
    }

    @Override
    public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
    {
      List<DiscoveryDruidNode> l = Lists.newArrayList(nodes);
      Collections.sort(l, (n1, n2) -> n1.getDruidNode().getHostAndPortToUse().compareTo(n2.getDruidNode().getHostAndPortToUse()));

      for (DiscoveryDruidNode node : l) {
        assertNextEvent(Event.added(node));
      }
    }

    @Override
    public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
    {
      List<DiscoveryDruidNode> l = Lists.newArrayList(nodes);
      Collections.sort(l, (n1, n2) -> n1.getDruidNode().getHostAndPortToUse().compareTo(n2.getDruidNode().getHostAndPortToUse()));

      for (DiscoveryDruidNode node : l) {
        assertNextEvent(Event.deleted(node));
      }
    }

    private void assertNextEvent(Event actual)
    {
      if (!failed && !events.isEmpty()) {
        Event expected = events.remove(0);
        failed = !actual.equals(expected);
        if (failed) {
          failReason = StringUtils.format("Failed Equals [%s] and [%s]", expected, actual);
        }
      }
    }

    public void assertSuccess() throws Exception
    {
      while (!events.isEmpty()) {
        Assert.assertFalse(failReason, failed);
        LOGGER.info("Waiting  for events to finish.");
        Thread.sleep(1000);
      }

      Assert.assertFalse(failReason, failed);
    }

    static class Event
    {
      String type;
      DiscoveryDruidNode node;

      private Event(String type, DiscoveryDruidNode node)
      {
        this.type = type;
        this.node = node;
      }

      static Event inited()
      {
        return new Event("inited", null);
      }

      static Event added(DiscoveryDruidNode node)
      {
        return new Event("added", node);
      }

      static Event deleted(DiscoveryDruidNode node)
      {
        return new Event("deleted", node);
      }

      @Override
      public boolean equals(Object o)
      {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        Event event = (Event) o;
        return type.equals(event.type) &&
               Objects.equals(node, event.node);
      }

      @Override
      public int hashCode()
      {
        return Objects.hash(type, node);
      }

      @Override
      public String toString()
      {
        return "Event{" +
               "type='" + type + '\'' +
               ", node=" + node +
               '}';
      }
    }
  }

  private static class MockWatchResult implements WatchResult
  {
    private List<Watch.Response<DiscoveryDruidNodeAndResourceVersion>> results;

    private volatile boolean timeoutOnStart;
    private volatile boolean timeooutOnEmptyResults;
    private volatile boolean closeCalled = false;

    public MockWatchResult(
        List<Watch.Response<DiscoveryDruidNodeAndResourceVersion>> results,
        boolean timeoutOnStart,
        boolean timeooutOnEmptyResults
    )
    {
      this.results = Lists.newArrayList(results);
      this.timeoutOnStart = timeoutOnStart;
      this.timeooutOnEmptyResults = timeooutOnEmptyResults;
    }

    @Override
    public boolean hasNext() throws SocketTimeoutException
    {
      if (timeoutOnStart) {
        throw new SocketTimeoutException("testing timeout on start!!!");
      }

      if (results.isEmpty()) {
        if (timeooutOnEmptyResults) {
          throw new SocketTimeoutException("testing timeout on end!!!");
        } else {
          try {
            Thread.sleep(Long.MAX_VALUE);
            return false; // just making compiler happy, will never reach this.
          }
          catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      } else {
        return true;
      }
    }

    @Override
    public Watch.Response<DiscoveryDruidNodeAndResourceVersion> next()
    {
      return results.remove(0);
    }

    @Override
    public void close()
    {
      closeCalled = true;
    }

    public void assertSuccess()
    {
      Assert.assertTrue("close() not called", closeCalled);
    }
  }
}
