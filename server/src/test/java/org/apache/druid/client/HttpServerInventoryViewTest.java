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

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

/**
 *
 */
public class HttpServerInventoryViewTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final String EXEC_NAME_PREFIX = "InventoryViewTest";

  private StubServiceEmitter serviceEmitter;
  private HttpServerInventoryView httpServerInventoryView;
  private TestSegmentChangeRequestHttpClient httpClient;
  private TestExecutorFactory executorFactory;

  private TestDruidNodeDiscovery druidNodeDiscovery;
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  @BeforeClass
  public static void setupClass()
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  @Before
  public void setup()
  {
    serviceEmitter = new StubServiceEmitter("test", "localhost");

    druidNodeDiscovery = new TestDruidNodeDiscovery();
    druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(DataNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    httpClient = new TestSegmentChangeRequestHttpClient();
    executorFactory = new TestExecutorFactory();
    httpServerInventoryView = new HttpServerInventoryView(
        MAPPER,
        httpClient,
        druidNodeDiscoveryProvider,
        pair -> !pair.rhs.getDataSource().equals("non-loading-datasource"),
        new HttpServerInventoryViewConfig(null, null, null),
        EXEC_NAME_PREFIX,
        executorFactory,
        serviceEmitter
    );
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(druidNodeDiscoveryProvider);
  }

  @Test(timeout = 60_000L)
  public void testSimple() throws Exception
  {
    final DataSegment segment1 = new DataSegment(
        "test1", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    final DataSegment segment2 = new DataSegment(
        "test2", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    final DataSegment segment3 = new DataSegment(
        "test3", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    final DataSegment segment4 = new DataSegment(
        "test4", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    final DataSegment segment5 = new DataSegment(
        "non-loading-datasource", Intervals.of("2014/2015"), "v1",
        null, null, null, null, 0, 0
    );

    CountDownLatch initializeCallback1 = new CountDownLatch(1);

    Map<SegmentId, CountDownLatch> segmentAddLatches = ImmutableMap.of(
        segment1.getId(), new CountDownLatch(1),
        segment2.getId(), new CountDownLatch(1),
        segment3.getId(), new CountDownLatch(1),
        segment4.getId(), new CountDownLatch(1)
    );

    Map<SegmentId, CountDownLatch> segmentDropLatches = ImmutableMap.of(
        segment1.getId(), new CountDownLatch(1),
        segment2.getId(), new CountDownLatch(1)
    );

    httpServerInventoryView.registerSegmentCallback(
        Execs.directExecutor(),
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            segmentAddLatches.get(segment.getId()).countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            segmentDropLatches.get(segment.getId()).countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            initializeCallback1.countDown();
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    final CountDownLatch serverRemovedCalled = new CountDownLatch(1);
    httpServerInventoryView.registerServerRemovedCallback(
        Execs.directExecutor(),
        server -> {
          if (server.getName().equals("host:8080")) {
            serverRemovedCalled.countDown();
            return ServerView.CallbackAction.CONTINUE;
          } else {
            throw new RE("Unknown server [%s]", server.getName());
          }
        }
    );

    httpServerInventoryView.start();

    // Complete and verify initialization
    final BlockingExecutorService syncExecutor = executorFactory.getSyncExecutor();
    syncExecutor.finishNextPendingTasks(1);
    initializeCallback1.await();

    // Add a node
    DiscoveryDruidNode druidNode = new DiscoveryDruidNode(
        new DruidNode("service", "host", false, 8080, null, true, false),
        NodeRole.HISTORICAL,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY,
            new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0)
        )
    );
    druidNodeDiscovery.listener.nodesAdded(ImmutableList.of(druidNode));

    // Request 1: Fail while sending to the server
    final TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>> typeRef
        = HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF;
    httpClient.addNextError(new ISE("Could not send request to server"));
    syncExecutor.finishNextPendingTask();

    // TODO: Request 2: Fail on the server-side
    httpClient.addNextError(new ISE("failure on server"));
    syncExecutor.finishNextPendingTask();

    // Request 3: Load S1
    httpClient.addNextResult(
        buildChangeSnapshot(new SegmentChangeRequestLoad(segment1)),
        typeRef
    );
    syncExecutor.finishNextPendingTasks(2);
    segmentAddLatches.get(segment1.getId()).await();

    // Request 4: Drop S1, Load S2, S3
    httpClient.addNextResult(
        buildChangeSnapshot(
            new SegmentChangeRequestDrop(segment1),
            new SegmentChangeRequestLoad(segment2),
            new SegmentChangeRequestLoad(segment3)
        ),
        typeRef
    );
    syncExecutor.finishNextPendingTasks(2);
    segmentDropLatches.get(segment1.getId()).await();
    segmentAddLatches.get(segment2.getId()).await();
    segmentAddLatches.get(segment3.getId()).await();

    // Request 5: reset the counter
    httpClient.addNextResult(
        new ChangeRequestsSnapshot<>(
            true,
            "Server requested reset",
            ChangeRequestHistory.Counter.ZERO,
            Collections.emptyList()
        ),
        typeRef
    );
    syncExecutor.finishNextPendingTasks(2);

    // Request 6: Load S3, S4, S5
    httpClient.addNextResult(
        buildChangeSnapshot(
            new SegmentChangeRequestLoad(segment3),
            new SegmentChangeRequestLoad(segment4),
            new SegmentChangeRequestLoad(segment5)
        ),
        typeRef
    );
    syncExecutor.finishNextPendingTasks(2);
    segmentDropLatches.get(segment2.getId()).await();
    segmentAddLatches.get(segment4.getId()).await();

    DruidServer druidServer = httpServerInventoryView.getInventoryValue("host:8080");
    Assert.assertEquals(
        ImmutableMap.of(segment3.getId(), segment3, segment4.getId(), segment4),
        Maps.uniqueIndex(druidServer.iterateAllSegments(), DataSegment::getId)
    );

    // Verify node removal
    druidNodeDiscovery.listener.nodesRemoved(ImmutableList.of(druidNode));

    // test removal event with empty services
    druidNodeDiscovery.listener.nodesRemoved(
        ImmutableList.of(
            new DiscoveryDruidNode(
                new DruidNode("service", "host", false, 8080, null, true, false),
                NodeRole.INDEXER,
                Collections.emptyMap()
            )
        )
    );

    // test removal rogue node (announced a service as a DataNodeService but wasn't a DataNodeService at the key)
    druidNodeDiscovery.listener.nodesRemoved(
        ImmutableList.of(
            new DiscoveryDruidNode(
                new DruidNode("service", "host", false, 8080, null, true, false),
                NodeRole.INDEXER,
                ImmutableMap.of(
                    DataNodeService.DISCOVERY_SERVICE_KEY,
                    new LookupNodeService("lookyloo")
                )
            )
        )
    );

    serverRemovedCalled.await();
    Assert.assertNull(httpServerInventoryView.getInventoryValue("host:8080"));

    httpServerInventoryView.stop();
  }

  @Test(timeout = 60_000L)
  public void testSyncMonitoring()
  {
    httpServerInventoryView.start();
    httpServerInventoryView.serverAdded(makeServer("abc.com:8080"));
    httpServerInventoryView.serverAdded(makeServer("xyz.com:8080"));
    httpServerInventoryView.serverAdded(makeServer("lol.com:8080"));
    Assert.assertEquals(3, httpServerInventoryView.getDebugInfo().size());
    httpServerInventoryView.checkAndResetUnhealthyServers();
    Assert.assertEquals(3, httpServerInventoryView.getDebugInfo().size());
  }

  @Test
  @Ignore
  public void testSyncStatusMetricsAreEmitted()
  {

  }

  @Test
  @Ignore
  public void testRemovedServerDoesNotDelayInitialization()
  {

  }

  private DruidServer makeServer(String host)
  {
    return new DruidServer(
        host,
        host,
        host,
        100_000_000L,
        ServerType.HISTORICAL,
        "__default_tier",
        50
    );
  }

  private ChangeRequestsSnapshot<DataSegmentChangeRequest> buildChangeSnapshot(
      DataSegmentChangeRequest... requests
  )
  {
    return new ChangeRequestsSnapshot<>(
        false,
        null,
        ChangeRequestHistory.Counter.ZERO,
        Arrays.asList(requests)
    );
  }

  private static class TestDruidNodeDiscovery implements DruidNodeDiscovery
  {
    Listener listener;

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public void registerListener(Listener listener)
    {
      listener.nodesAdded(ImmutableList.of());
      listener.nodeViewInitialized();
      this.listener = listener;
    }
  }

  private static class TestExecutorFactory implements ScheduledExecutorFactory
  {
    private final Map<String, BlockingExecutorService> executors = new HashMap<>();

    @Override
    public ScheduledExecutorService create(int corePoolSize, String nameFormat)
    {
      BlockingExecutorService executorService = new BlockingExecutorService(nameFormat);
      executors.put(nameFormat, executorService);
      return new WrappingScheduledExecutorService(nameFormat, executorService, false);
    }

    BlockingExecutorService getMonitoringExecutor()
    {
      return executors.get(EXEC_NAME_PREFIX + "-monitor-%s");
    }

    BlockingExecutorService getSyncExecutor()
    {
      return executors.get(EXEC_NAME_PREFIX + "-%s");
    }
  }

}
