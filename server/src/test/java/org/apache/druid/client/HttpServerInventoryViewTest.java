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
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServerInventoryViewTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>>
      TYPE_REF = HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF;

  private static final String EXEC_NAME_PREFIX = "InventoryViewTest";

  private static final String METRIC_SUCCESS = "serverview/sync/healthy";
  private static final String METRIC_UNSTABLE_TIME = "serverview/sync/unstableTime";

  private StubServiceEmitter serviceEmitter;

  private HttpServerInventoryView httpServerInventoryView;
  private TestChangeRequestHttpClient<ChangeRequestsSnapshot<DataSegmentChangeRequest>> httpClient;
  private TestExecutorFactory execHelper;

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

    httpClient = new TestChangeRequestHttpClient<>(TYPE_REF, MAPPER);
    execHelper = new TestExecutorFactory();
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

    execHelper.finishInventoryInitialization();
    Assert.assertTrue(inventoryInitialized.get());

    httpServerInventoryView.stop();
  }

  @Test
  public void testStopShutsDownExecutors()
  {
    httpServerInventoryView.start();
    Assert.assertFalse(execHelper.syncExecutor.isShutdown());

    httpServerInventoryView.stop();
    Assert.assertTrue(execHelper.syncExecutor.isShutdown());
  }

  @Test
  public void testAddNodeStartsSync()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    final DiscoveryDruidNode druidNode = druidNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final DruidServer server = druidNode.toDruidServer();

    Collection<DruidServer> inventory = httpServerInventoryView.getInventory();
    Assert.assertEquals(1, inventory.size());
    Assert.assertTrue(inventory.contains(server));

    execHelper.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 1);
    serviceEmitter.verifyNotEmitted(METRIC_UNSTABLE_TIME);

    DataSegment segment = CreateDataSegments.ofDatasource("wiki").eachOfSizeInMb(500).get(0);
    httpClient.completeNextRequestWith(
        snapshotOf(new SegmentChangeRequestLoad(segment))
    );
    execHelper.sendSyncRequestAndHandleResponse();

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
    execHelper.finishInventoryInitialization();

    final DiscoveryDruidNode druidNode = druidNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final DruidServer server = druidNode.toDruidServer();

    druidNodeDiscovery.removeNodesAndNotifyListeners(druidNode);

    Assert.assertNull(httpServerInventoryView.getInventoryValue(server.getName()));

    execHelper.emitMetrics();
    serviceEmitter.verifyNotEmitted(METRIC_SUCCESS);
    serviceEmitter.verifyNotEmitted(METRIC_UNSTABLE_TIME);

    httpServerInventoryView.stop();
  }

  @Test(timeout = 60_000L)
  public void testSyncSegmentLoadAndDrop()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

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
        snapshotOf(new SegmentChangeRequestLoad(segments[0]))
    );
    execHelper.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(isAddedToView(server, segments[0]));

    // Request 2: Drop S1, Load S2, S3
    resetForNextSyncRequest();
    httpClient.completeNextRequestWith(
        snapshotOf(
            new SegmentChangeRequestDrop(segments[0]),
            new SegmentChangeRequestLoad(segments[1]),
            new SegmentChangeRequestLoad(segments[2])
        )
    );
    execHelper.sendSyncRequestAndHandleResponse();
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
        )
    );
    execHelper.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(segmentsAddedToView.isEmpty());
    Assert.assertTrue(segmentsRemovedFromView.isEmpty());

    // Request 4: Load S3, S4
    resetForNextSyncRequest();
    httpClient.completeNextRequestWith(
        snapshotOf(
            new SegmentChangeRequestLoad(segments[2]),
            new SegmentChangeRequestLoad(segments[3])
        )
    );
    execHelper.sendSyncRequestAndHandleResponse();
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
    execHelper.finishInventoryInitialization();

    druidNodeDiscovery.addNodeAndNotifyListeners("localhost");

    httpClient.failToSendNextRequestWith(new ISE("Could not send request to server"));
    execHelper.sendSyncRequest();

    serviceEmitter.flush();
    execHelper.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 0);

    httpServerInventoryView.stop();
  }

  @Test
  public void testSyncWhenErrorResponse()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    druidNodeDiscovery.addNodeAndNotifyListeners("localhost");

    httpClient.completeNextRequestWith(InvalidInput.exception("failure on server"));
    execHelper.sendSyncRequestAndHandleResponse();

    serviceEmitter.flush();
    execHelper.emitMetrics();
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
    execHelper.finishInventoryInitialization();

    druidNodeDiscovery.addNodeAndNotifyListeners("localhost");

    serviceEmitter.flush();
    httpClient.completeNextRequestWith(InvalidInput.exception("failure on server"));
    execHelper.sendSyncRequestAndHandleResponse();

    List<AlertEvent> alerts = serviceEmitter.getAlerts();
    Assert.assertEquals(1, alerts.size());
    AlertEvent alert = alerts.get(0);
    Assert.assertTrue(alert.getDescription().contains("Sync failed for server"));

    serviceEmitter.flush();
    execHelper.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 0);

    httpServerInventoryView.stop();
  }

  @Test(timeout = 60_000)
  public void testInitWaitsForServerToSync()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    druidNodeDiscovery.addNodeAndNotifyListeners("localhost");

    ExecutorService initExecutor = Execs.singleThreaded(EXEC_NAME_PREFIX + "-init");

    try {
      initExecutor.submit(() -> execHelper.finishInventoryInitialization());

      // Wait to ensure that init thread is in progress and waiting
      Thread.sleep(1000);
      Assert.assertFalse(inventoryInitialized.get());

      // Finish sync of server
      httpClient.completeNextRequestWith(snapshotOf());
      execHelper.sendSyncRequestAndHandleResponse();

      // Wait for 10 seconds to ensure that init thread knows about server sync
      Thread.sleep(10_000);
      Assert.assertTrue(inventoryInitialized.get());
    }
    catch (InterruptedException e) {
      throw new ISE(e, "Interrupted");
    }
    finally {
      initExecutor.shutdownNow();
    }
  }

  @Test(timeout = 60_000)
  public void testInitDoesNotWaitForRemovedServerToSync()
  {
    httpServerInventoryView.start();
    druidNodeDiscovery.markNodeViewInitialized();
    DiscoveryDruidNode node = druidNodeDiscovery.addNodeAndNotifyListeners("localhost");

    ExecutorService initExecutor = Execs.singleThreaded(EXEC_NAME_PREFIX + "-init");

    try {
      initExecutor.submit(() -> execHelper.finishInventoryInitialization());

      // Wait to ensure that init thread is in progress and waiting
      Thread.sleep(1000);
      Assert.assertFalse(inventoryInitialized.get());

      // Remove the node from discovery
      druidNodeDiscovery.removeNodesAndNotifyListeners(node);

      // Wait for 10 seconds to ensure that init thread knows about server removal
      Thread.sleep(10_000);
      Assert.assertTrue(inventoryInitialized.get());
    }
    catch (InterruptedException e) {
      throw new ISE(e, "Interrupted");
    }
    finally {
      initExecutor.shutdownNow();
    }
  }

  private void createInventoryView(HttpServerInventoryViewConfig config)
  {
    httpServerInventoryView = new HttpServerInventoryView(
        MAPPER,
        httpClient,
        druidNodeDiscoveryProvider,
        pair -> !pair.rhs.getDataSource().equals("non-loading-datasource"),
        config,
        serviceEmitter,
        execHelper,
        EXEC_NAME_PREFIX
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

          @Override
          public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
          {
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

  private static ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshotOf(
      DataSegmentChangeRequest... requests
  )
  {
    return ChangeRequestsSnapshot.success(
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
      listener.nodesAdded(ImmutableList.of(discoveryDruidNode));

      return discoveryDruidNode;
    }

    void removeNodesAndNotifyListeners(DiscoveryDruidNode... nodesToRemove)
    {
      listener.nodesRemoved(Arrays.asList(nodesToRemove));
    }
  }

  /**
   * Creates and retains a handle on the executors used by the inventory view.
   * <p>
   * There are 4 types of tasks submitted to the two executors. Upon succesful
   * completion, each of these tasks add another task to the execution queue.
   * <p>
   * Tasks running on sync executor:
   * <ol>
   *   <li>send request to server (adds "handle response" to queue)</li>
   *   <li>handle response and execute callbacks (adds "send request" to queue)</li>
   * </ol>
   * <p>
   * Tasks running on monitoring executor.
   * <ol>
   * <li>check and reset unhealthy servers (adds self to queue)</li>
   * <li>emit metrics (adds self to queue)</li>
   * </ol>
   */
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

    void finishInventoryInitialization()
    {
      syncExecutor.finishNextPendingTask();
    }

    void emitMetrics()
    {
      // Finish 1 task for check and reset, 1 for metric emission
      monitorExecutor.finishNextPendingTasks(2);
    }
  }
}
