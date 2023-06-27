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
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.granularity.Granularities;
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
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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

  private Map<DruidServerMetadata, Set<DataSegment>> segmentsAddedToView;
  private Map<DruidServerMetadata, Set<DataSegment>> segmentsRemovedFromView;

  private AtomicBoolean inventoryInitialized;

  /**
   * TODO:
   * initialize with proper filter that can be updated later
   * required items
   * allow a request to fail on server side
   * control response code and content
   */

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

    inventoryInitialized = new AtomicBoolean(false);
    segmentsAddedToView = new HashMap<>();
    segmentsRemovedFromView = new HashMap<>();

    httpServerInventoryView.registerSegmentCallback(
        Execs.directExecutor(),
        ServerView.perpetualSegmentCallback(
            (server, segment) -> segmentsAddedToView.computeIfAbsent(server, s -> new HashSet<>())
                                                    .add(segment),
            (server, segment) -> segmentsRemovedFromView.computeIfAbsent(server, s -> new HashSet<>())
                                                        .add(segment),
            () -> inventoryInitialized.set(true)
        )
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
    executorFactory.initializeSync();
    Assert.assertTrue(inventoryInitialized.get());

    // Add a node
    DruidNode druidNode = new DruidNode("service", "host", false, 8080, null, true, false);
    DataNodeService dataNodeService = new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0);
    DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        druidNode,
        NodeRole.HISTORICAL,
        ImmutableMap.of(DataNodeService.DISCOVERY_SERVICE_KEY, dataNodeService)
    );
    druidNodeDiscovery.listener.nodesAdded(ImmutableList.of(discoveryDruidNode));

    final DruidServer server = new DruidServer(
        druidNode.getHostAndPortToUse(),
        druidNode.getHostAndPort(),
        druidNode.getHostAndTlsPort(),
        dataNodeService.getMaxSize(),
        dataNodeService.getServerType(),
        dataNodeService.getTier(),
        dataNodeService.getPriority()
    );

    // Request 1: Fail while sending to the server
    final TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>> typeRef
        = HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF;
    httpClient.addNextError(new ISE("Could not send request to server"));
    executorFactory.executeSync();

    // TODO: Request 2: Fail on the server-side
    httpClient.addNextError(new ISE("failure on server"));
    executorFactory.executeSync();

    // Prepare segments
    final DataSegment[] segments =
        CreateDataSegments.ofDatasource("wiki")
                          .forIntervals(4, Granularities.DAY)
                          .eachOfSizeInMb(500)
                          .toArray(new DataSegment[0]);

    // Request 3: Load S1
    httpClient.addNextResult(
        buildChangeSnapshot(new SegmentChangeRequestLoad(segments[0])),
        typeRef
    );
    executorFactory.executeSyncAndCallback();
    Assert.assertTrue(isAddedToView(server, segments[0]));

    // Request 4: Drop S1, Load S2, S3
    resetForNextSyncRequest();
    httpClient.addNextResult(
        buildChangeSnapshot(
            new SegmentChangeRequestDrop(segments[0]),
            new SegmentChangeRequestLoad(segments[1]),
            new SegmentChangeRequestLoad(segments[2])
        ),
        typeRef
    );
    executorFactory.executeSyncAndCallback();
    Assert.assertTrue(isRemovedFromView(server, segments[0]));
    Assert.assertTrue(isAddedToView(server, segments[1]));
    Assert.assertTrue(isAddedToView(server, segments[2]));

    // Request 5: reset the counter
    resetForNextSyncRequest();
    httpClient.addNextResult(
        new ChangeRequestsSnapshot<>(
            true,
            "Server requested reset",
            ChangeRequestHistory.Counter.ZERO,
            Collections.emptyList()
        ),
        typeRef
    );
    executorFactory.executeSyncAndCallback();
    Assert.assertTrue(segmentsAddedToView.isEmpty());
    Assert.assertTrue(segmentsRemovedFromView.isEmpty());

    // Request 6: Load S3, S4
    httpClient.addNextResult(
        buildChangeSnapshot(
            new SegmentChangeRequestLoad(segments[2]),
            new SegmentChangeRequestLoad(segments[3])
        ),
        typeRef
    );
    executorFactory.executeSyncAndCallback();
    Assert.assertTrue(isRemovedFromView(server, segments[1]));
    Assert.assertTrue(isAddedToView(server, segments[3]));

    DruidServer druidServer = httpServerInventoryView.getInventoryValue("host:8080");
    Assert.assertEquals(2, druidServer.getTotalSegments());
    Assert.assertNotNull(druidServer.getSegment(segments[2].getId()));
    Assert.assertNotNull(druidServer.getSegment(segments[3].getId()));

    // Verify node removal
    druidNodeDiscovery.listener.nodesRemoved(ImmutableList.of(discoveryDruidNode));

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

  @Test
  public void testSyncWithFailureOnClientSide()
  {

  }

  @Test
  public void testSyncWithFailureOnServerSide()
  {

  }

  @Test
  public void testSyncWithFilteredInventoryView()
  {

  }

  @Test
  public void testSyncWithMultipleServers()
  {

  }

  @Test
  @Ignore
  public void testRemovedServerDoesNotDelayInitialization()
  {

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
    // this might just fit into the rest of the tests
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

  private static ChangeRequestsSnapshot<DataSegmentChangeRequest> buildChangeSnapshot(
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

  private boolean isAddedToView(DruidServer server, DataSegment segment)
  {
    return segmentsAddedToView.getOrDefault(server.getMetadata(), Collections.emptySet())
                              .contains(segment);
  }

  private boolean isRemovedFromView(DruidServer server, DataSegment segment)
  {
    return segmentsRemovedFromView.getOrDefault(server.getMetadata(), Collections.emptySet())
                                  .contains(segment);
  }

  private void resetForNextSyncRequest()
  {
    segmentsAddedToView.clear();
    segmentsRemovedFromView.clear();
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
    private BlockingExecutorService syncExecutor;
    private BlockingExecutorService monitorExecutor;

    @Override
    public ScheduledExecutorService create(int corePoolSize, String nameFormat)
    {
      BlockingExecutorService executorService = new BlockingExecutorService(nameFormat);
      final String syncExecutorPrefix = EXEC_NAME_PREFIX + "-%s";
      final String monitorExecutorPrefix = EXEC_NAME_PREFIX + "-monitor-%s";
      if (syncExecutorPrefix.equals(nameFormat)) {
        syncExecutor = executorService;
      } else if (monitorExecutorPrefix.equals(nameFormat)) {
        monitorExecutor = executorService;
      }

      return new WrappingScheduledExecutorService(nameFormat, executorService, false);
    }

    void executeSyncAndCallback()
    {
      syncExecutor.finishNextPendingTasks(2);
    }

    void executeSync()
    {
      syncExecutor.finishNextPendingTask();
    }

    void initializeSync()
    {
      syncExecutor.finishNextPendingTask();
    }
  }

}
