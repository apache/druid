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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.AlertEvent;
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
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class HttpServerInventoryViewTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>>
      TYPE_REF = HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF;

  private static final String EXEC_NAME_PREFIX = "InventoryViewTest";

  private static final String METRIC_SUCCESS = "segment/serverview/sync/success";
  private static final String METRIC_UNSTABLE_TIME = "segment/serverview/sync/unstableTime";

  private StubServiceEmitter serviceEmitter;

  private HttpServerInventoryView httpServerInventoryView;
  private TestSegmentChangeRequestHttpClient httpClient;
  private TestExecutorFactory executorFactory;

  private TestDruidNodeDiscovery druidNodeDiscovery;
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  private Map<DruidServerMetadata, Set<DataSegment>> segmentsAddedToView;
  private Map<DruidServerMetadata, Set<DataSegment>> segmentsRemovedFromView;
  private Set<DruidServerMetadata> removedServers;

  private AtomicBoolean inventoryInitialized;

  @Before
  public void setup()
  {
    serviceEmitter = new StubServiceEmitter("test", "localhost");
    EmittingLogger.registerEmitter(serviceEmitter);

    druidNodeDiscovery = new TestDruidNodeDiscovery();
    druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(DataNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    httpClient = new TestSegmentChangeRequestHttpClient();
    executorFactory = new TestExecutorFactory();
    inventoryInitialized = new AtomicBoolean(false);

    segmentsAddedToView = new HashMap<>();
    segmentsRemovedFromView = new HashMap<>();
    removedServers = new HashSet<>();

    createInventoryView(
        new HttpServerInventoryViewConfig(null, null, null)
    );
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(druidNodeDiscoveryProvider);
    if (httpServerInventoryView != null && httpServerInventoryView.isStarted()) {
      httpServerInventoryView.stop();
    }
  }

  @Test
  public void testInitHappensAfterNodeViewInit()
  {
    httpServerInventoryView.start();
    Assert.assertTrue(httpServerInventoryView.isStarted());
    Assert.assertFalse(inventoryInitialized.get());

    druidNodeDiscovery.markNodeViewInitialized();
    Assert.assertFalse(inventoryInitialized.get());

    executorFactory.executeInventoryInitTask();
    Assert.assertTrue(inventoryInitialized.get());

    httpServerInventoryView.stop();
  }

  @Test
  public void testStopShutsDownExecutors()
  {
    httpServerInventoryView.start();
    Assert.assertFalse(executorFactory.syncExecutor.isShutdown());

    httpServerInventoryView.stop();
    Assert.assertTrue(executorFactory.syncExecutor.isShutdown());
  }

  @Test
  public void testAddNodeStartsSync()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    executorFactory.executeInventoryInitTask();

    final DiscoveryDruidNode druidNode = druidNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final DruidServer server = druidNode.toDruidServer();

    Collection<DruidServer> inventory = httpServerInventoryView.getInventory();
    Assert.assertEquals(1, inventory.size());
    Assert.assertTrue(inventory.contains(server));

    executorFactory.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 1);
    serviceEmitter.verifyNotEmitted(METRIC_UNSTABLE_TIME);

    DataSegment segment = CreateDataSegments.ofDatasource("wiki").eachOfSizeInMb(500).get(0);
    httpClient.completeNextRequestWith(
        buildChangeSnapshot(new SegmentChangeRequestLoad(segment)),
        TYPE_REF
    );
    executorFactory.sendSyncRequestAndHandleResponse();

    DruidServer inventoryValue = httpServerInventoryView.getInventoryValue(server.getName());
    Assert.assertNotNull(inventoryValue);
    Assert.assertEquals(1, inventoryValue.getTotalSegments());
    Assert.assertNotNull(inventoryValue.getSegment(segment.getId()));

    httpServerInventoryView.stop();
  }

  @Test
  public void testRemoveNodeStopsSync()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    executorFactory.executeInventoryInitTask();

    final DiscoveryDruidNode druidNode = druidNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final DruidServer server = druidNode.toDruidServer();

    druidNodeDiscovery.removeNodesAndNotifyListeners(druidNode);

    Assert.assertNull(httpServerInventoryView.getInventoryValue(server.getName()));

    executorFactory.emitMetrics();
    serviceEmitter.verifyNotEmitted(METRIC_SUCCESS);
    serviceEmitter.verifyNotEmitted(METRIC_UNSTABLE_TIME);

    httpServerInventoryView.stop();
  }

  @Test(timeout = 60_000L)
  public void testSyncSegmentLoadAndDrop()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    executorFactory.executeInventoryInitTask();

    final DiscoveryDruidNode druidNode = druidNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final DruidServer server = druidNode.toDruidServer();

    final DataSegment[] segments =
        CreateDataSegments.ofDatasource("wiki")
                          .forIntervals(4, Granularities.DAY)
                          .eachOfSizeInMb(500)
                          .toArray(new DataSegment[0]);

    // Request 1: Load S1
    httpClient.completeNextRequestWith(
        buildChangeSnapshot(new SegmentChangeRequestLoad(segments[0])),
        TYPE_REF
    );
    executorFactory.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(isAddedToView(server, segments[0]));

    // Request 2: Drop S1, Load S2, S3
    resetForNextSyncRequest();
    httpClient.completeNextRequestWith(
        buildChangeSnapshot(
            new SegmentChangeRequestDrop(segments[0]),
            new SegmentChangeRequestLoad(segments[1]),
            new SegmentChangeRequestLoad(segments[2])
        ),
        TYPE_REF
    );
    executorFactory.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(isRemovedFromView(server, segments[0]));
    Assert.assertTrue(isAddedToView(server, segments[1]));
    Assert.assertTrue(isAddedToView(server, segments[2]));

    // Request 3: reset the counter
    resetForNextSyncRequest();
    httpClient.completeNextRequestWith(
        new ChangeRequestsSnapshot<>(
            true,
            "Server requested reset",
            ChangeRequestHistory.Counter.ZERO,
            Collections.emptyList()
        ),
        TYPE_REF
    );
    executorFactory.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(segmentsAddedToView.isEmpty());
    Assert.assertTrue(segmentsRemovedFromView.isEmpty());

    // Request 4: Load S3, S4
    resetForNextSyncRequest();
    httpClient.completeNextRequestWith(
        buildChangeSnapshot(
            new SegmentChangeRequestLoad(segments[2]),
            new SegmentChangeRequestLoad(segments[3])
        ),
        TYPE_REF
    );
    executorFactory.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(isRemovedFromView(server, segments[1]));
    Assert.assertTrue(isAddedToView(server, segments[3]));

    DruidServer inventoryValue = httpServerInventoryView.getInventoryValue(server.getName());
    Assert.assertNotNull(inventoryValue);
    Assert.assertEquals(2, inventoryValue.getTotalSegments());
    Assert.assertNotNull(inventoryValue.getSegment(segments[2].getId()));
    Assert.assertNotNull(inventoryValue.getSegment(segments[3].getId()));

    // Verify node removal
    druidNodeDiscovery.removeNodesAndNotifyListeners(druidNode);

    // test removal event with empty services
    druidNodeDiscovery.removeNodesAndNotifyListeners(
        new DiscoveryDruidNode(
            new DruidNode("service", "host", false, 8080, null, true, false),
            NodeRole.INDEXER,
            Collections.emptyMap()
        )
    );

    // test removal rogue node (announced a service as a DataNodeService but wasn't a DataNodeService at the key)
    druidNodeDiscovery.removeNodesAndNotifyListeners(
        new DiscoveryDruidNode(
            new DruidNode("service", "host", false, 8080, null, true, false),
            NodeRole.INDEXER,
            ImmutableMap.of(
                DataNodeService.DISCOVERY_SERVICE_KEY,
                new LookupNodeService("lookyloo")
            )
        )
    );

    Assert.assertTrue(removedServers.contains(server.getMetadata()));
    Assert.assertNull(httpServerInventoryView.getInventoryValue(server.getName()));

    httpServerInventoryView.stop();
  }

  @Test
  public void testSyncWhenRequestFailedToSend()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    executorFactory.executeInventoryInitTask();

    druidNodeDiscovery.addNodeAndNotifyListeners("localhost");

    httpClient.failNextRequestOnClientWith(new ISE("Could not send request to server"));
    executorFactory.sendSyncRequest();

    executorFactory.emitMetrics();
    serviceEmitter.verifyEmitted(METRIC_UNSTABLE_TIME, 1);
    serviceEmitter.verifyValue(METRIC_SUCCESS, 0);

    httpServerInventoryView.stop();
  }

  @Test
  public void testSyncWhenErrorResponse()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    executorFactory.executeInventoryInitTask();

    druidNodeDiscovery.addNodeAndNotifyListeners("localhost");

    httpClient.failNextRequestOnServerWith(InvalidInput.exception("failure on server"));
    executorFactory.sendSyncRequestAndHandleResponse();

    executorFactory.emitMetrics();
    serviceEmitter.verifyEmitted(METRIC_UNSTABLE_TIME, 1);
    serviceEmitter.verifyValue(METRIC_SUCCESS, 0);

    httpServerInventoryView.stop();
  }

  @Test
  public void testUnstableServerAlertsAfterTimeout()
  {
    // Create inventory with alert timeout as 0 ms
    createInventoryView(
        new HttpServerInventoryViewConfig(null, Period.millis(0), null)
    );

    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    executorFactory.executeInventoryInitTask();

    druidNodeDiscovery.addNodeAndNotifyListeners("localhost");

    httpClient.failNextRequestOnServerWith(InvalidInput.exception("failure on server"));
    executorFactory.sendSyncRequestAndHandleResponse();

    executorFactory.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 0);

    List<AlertEvent> alerts = serviceEmitter.getAlerts();
    Assert.assertEquals(1, alerts.size());
    AlertEvent alert = alerts.get(0);
    Assert.assertTrue(alert.getDescription().contains("Sync failed for server"));

    httpServerInventoryView.stop();
  }

  private void createInventoryView(HttpServerInventoryViewConfig config)
  {
    httpServerInventoryView = new HttpServerInventoryView(
        MAPPER,
        httpClient,
        druidNodeDiscoveryProvider,
        pair -> !pair.rhs.getDataSource().equals("non-loading-datasource"),
        config,
        EXEC_NAME_PREFIX,
        executorFactory,
        serviceEmitter
    );

    httpServerInventoryView.registerSegmentCallback(
        Execs.directExecutor(),
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            segmentsAddedToView.computeIfAbsent(server, s -> new HashSet<>()).add(segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            segmentsRemovedFromView.computeIfAbsent(server, s -> new HashSet<>()).add(segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            inventoryInitialized.set(true);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    httpServerInventoryView.registerServerRemovedCallback(
        Execs.directExecutor(),
        server -> {
          removedServers.add(server.getMetadata());
          return ServerView.CallbackAction.CONTINUE;
        }
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
    final Set<DiscoveryDruidNode> nodes = new HashSet<>();

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return nodes;
    }

    @Override
    public void registerListener(Listener listener)
    {
      this.listener = listener;
    }

    /**
     * Marks the node view as initialized and notifies the listeners.
     */
    void markNodeViewInitialized()
    {
      listener.nodeViewInitialized();
    }

    /**
     * Creates and adds a new node and notifies the listeners.
     */
    DiscoveryDruidNode addNodeAndNotifyListeners(String host)
    {
      final DruidNode druidNode = new DruidNode("druid/historical", host, false, 8080, null, true, false);
      DataNodeService dataNodeService = new DataNodeService("tier", 10L << 30, ServerType.HISTORICAL, 0);
      final DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
          druidNode,
          NodeRole.HISTORICAL,
          ImmutableMap.of(DataNodeService.DISCOVERY_SERVICE_KEY, dataNodeService)
      );
      nodes.add(discoveryDruidNode);
      listener.nodesAdded(ImmutableList.of(discoveryDruidNode));

      return discoveryDruidNode;
    }

    void removeNodesAndNotifyListeners(DiscoveryDruidNode... nodesToRemove)
    {
      List<DiscoveryDruidNode> nodeList = Arrays.asList(nodesToRemove);
      this.nodes.removeAll(nodeList);
      listener.nodesRemoved(nodeList);
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

    void sendSyncRequestAndHandleResponse()
    {
      syncExecutor.finishNextPendingTasks(2);
    }

    void sendSyncRequest()
    {
      syncExecutor.finishNextPendingTask();
    }

    void executeInventoryInitTask()
    {
      syncExecutor.finishNextPendingTask();
    }

    void emitMetrics()
    {
      monitorExecutor.finishNextPendingTask();
      monitorExecutor.finishNextPendingTask();
    }
  }

}
