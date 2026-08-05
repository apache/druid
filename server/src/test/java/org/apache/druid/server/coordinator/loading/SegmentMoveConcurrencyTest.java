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

package org.apache.druid.server.coordinator.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.DataSegmentChangeCallback;
import org.apache.druid.server.coordination.DataSegmentChangeHandler;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.DataSegmentChangeResponse;
import org.apache.druid.server.coordination.SegmentChangeStatus;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.server.http.SegmentLoadingCapabilities;
import org.apache.druid.server.http.SegmentLoadingMode;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Regression tests for apache/druid#18764.
 * <p>
 * A historical acknowledges a load or drop long before the Coordinator's inventory view reflects it. Forgetting the
 * operation at ack time leaves a window in which it is visible in neither the load queue nor the inventory, so the
 * replica looks plainly loaded; during a move that reads as over-replication and the Coordinator drops the last
 * remaining replica. These tests reproduce that window: the server acks, but the inventory is not advanced.
 */
public class SegmentMoveConcurrencyTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final String TIER = "tier1";

  private final DataSegment segment =
      CreateDataSegments.ofDatasource("test")
                        .forIntervals(1, Granularities.DAY)
                        .startingAt("2022-01-01")
                        .eachOfSizeInMb(100)
                        .get(0);

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;
  private SegmentLoadQueueManager loadQueueManager;

  private TestServer serverA;
  private TestServer serverB;

  @Before
  public void setUp()
  {
    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "SegmentMoveConcurrencyTest-%d"));
    balancerStrategy = new CostBalancerStrategy(exec);
    loadQueueManager = new SegmentLoadQueueManager(null, new TestTaskMaster());

    serverA = new TestServer("histA");
    serverB = new TestServer("histB");

    // Starting state: the segment has exactly one replica, on server A.
    serverA.druidServer.addDataSegment(segment);
  }

  @After
  public void tearDown()
  {
    serverA.peon.stop();
    serverB.peon.stop();
    exec.shutdown();
  }

  @Test
  public void testNoDropIsQueuedWhileTheSourceDropAwaitsInventoryConfirmation()
  {
    moveSegmentFromAtoB();

    // B acks and its inventory catches up, so the load callback queues the drop on the source.
    serverB.ackPendingRequests();
    serverB.confirmInventory(true);
    Assert.assertTrue(serverA.peon.getSegmentsToDrop().contains(segment));
    Assert.assertTrue(serverA.peon.getSegmentsMarkedToDrop().isEmpty());

    // A acks the drop but its inventory still reports the segment loaded: the state in the issue's trace between
    // "Server[A] completed request[DROP]" and the next inventory sync.
    serverA.ackPendingRequests();
    Assert.assertTrue(serverA.peon.getSegmentsToDrop().isEmpty());
    Assert.assertNotNull(serverA.druidServer.getSegment(segment.getId()));

    final DruidCluster cluster = buildCluster();
    final SegmentReplicaCount replicaCount = SegmentReplicaCountMap.create(cluster).getTotal(segment.getId());

    // Both servers still report the segment loaded, but the in-flight drop on A must still be accounted for.
    Assert.assertEquals(2, replicaCount.totalLoaded());
    Assert.assertEquals(1, replicaCount.loadedNotDropping());

    // With only one replica effectively loaded, there is no surplus and so no drop of B's replica.
    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster);
    params.getSegmentAssigner().replicateSegment(segment, ImmutableMap.of(TIER, 1));

    Assert.assertEquals(
        0L,
        params.getCoordinatorStats().getSegmentStat(Stats.Segments.DROPPED, TIER, segment.getDataSource())
    );
    Assert.assertTrue(serverB.peon.getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testNoDropIsQueuedWhileTheSourceIsStillMarkedToDrop()
  {
    moveSegmentFromAtoB();

    // B acks and its inventory catches up, but its load callback has not run, so A is still marked MOVE_FROM.
    serverB.ackServerOnly();
    serverB.confirmInventory(true);

    final DruidCluster cluster = buildCluster();
    final SegmentReplicaCount replicaCount = SegmentReplicaCountMap.create(cluster).getTotal(segment.getId());

    Assert.assertEquals(2, replicaCount.totalLoaded());
    Assert.assertEquals(1, replicaCount.moveCompletedPendingDrop());

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster);
    params.getSegmentAssigner().replicateSegment(segment, ImmutableMap.of(TIER, 1));

    Assert.assertEquals(
        0L,
        params.getCoordinatorStats().getSegmentStat(Stats.Segments.DROPPED, TIER, segment.getDataSource())
    );
    Assert.assertTrue(serverB.peon.getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testPendingSetTracksInFlightOperationsNotLoadedSegments()
  {
    // The retained-until-confirmed set must scale with load queue depth, not with segments held.
    final List<DataSegment> manySegments =
        CreateDataSegments.ofDatasource("scale")
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2024-01-01")
                          .withNumPartitions(200)
                          .eachOfSizeInMb(1);

    for (DataSegment s : manySegments) {
      serverB.peon.loadSegment(s, SegmentAction.LOAD, null);
    }
    serverB.ackPendingRequests();
    Assert.assertEquals(manySegments.size(), serverB.peon.getNumSegmentsAwaitingConfirmation());

    // Once the inventory reflects them they are all retired: 200 segments held, none tracked.
    manySegments.forEach(serverB.druidServer::addDataSegment);
    serverB.peon.getQueueSnapshot(serverB.druidServer.toImmutableDruidServer());

    Assert.assertEquals(0, serverB.peon.getNumSegmentsAwaitingConfirmation());
    Assert.assertEquals(manySegments.size(), serverB.druidServer.getTotalSegments());
  }

  /**
   * Queues the move via the production code path, leaving both requests sent but not yet acknowledged.
   */
  private void moveSegmentFromAtoB()
  {
    final DruidCluster cluster = buildCluster();
    final ServerHolder holderA = findHolder(cluster, serverA);
    final ServerHolder holderB = findHolder(cluster, serverB);

    Assert.assertTrue(loadQueueManager.moveSegment(segment, holderA, holderB, null));
    Assert.assertTrue(serverA.peon.getSegmentsMarkedToDrop().contains(segment));
  }

  private DruidCluster buildCluster()
  {
    return DruidCluster.builder().addTier(TIER, serverA.toHolder(), serverB.toHolder()).build();
  }

  private ServerHolder findHolder(DruidCluster cluster, TestServer server)
  {
    return cluster.getManagedHistoricalsByTier(TIER)
                  .stream()
                  .filter(h -> h.getServer().getName().equals(server.druidServer.getName()))
                  .findFirst()
                  .orElseThrow(() -> new RE("No holder for server[%s]", server.druidServer.getName()));
  }

  private DruidCoordinatorRuntimeParams makeRuntimeParams(DruidCluster cluster)
  {
    return DruidCoordinatorRuntimeParams
        .builder()
        .withDruidCluster(cluster)
        .withBalancerStrategy(balancerStrategy)
        .withUsedSegments(segment)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withSmartSegmentLoading(false)
                                    .withUseRoundRobinSegmentAssignment(false)
                                    .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();
  }

  /**
   * Answers the only question {@link SegmentLoadQueueManager#moveSegment} asks: whether loading is HTTP-based.
   */
  private static class TestTaskMaster extends LoadQueueTaskMaster
  {
    TestTaskMaster()
    {
      super(MAPPER, null, null, new HttpLoadQueuePeonConfig(null, null, null), null, null);
    }
  }

  /**
   * A historical with a real {@link HttpLoadQueuePeon} whose server responses and inventory updates are driven
   * independently, so tests can hold the inventory back the way a real sync period does.
   */
  private class TestServer implements DataSegmentChangeHandler
  {
    private final DruidServer druidServer;
    private final HttpLoadQueuePeon peon;
    private final BlockingExecutorService processingExecutor;
    private final BlockingExecutorService callbackExecutor;
    private final List<DataSegment> segmentsSentToServer = new ArrayList<>();

    TestServer(String name)
    {
      this.druidServer = new DruidServer(name, name, null, 10L << 30, null, ServerType.HISTORICAL, TIER, 0);
      this.processingExecutor = new BlockingExecutorService(name + "-processing");
      this.callbackExecutor = new BlockingExecutorService(name + "-callback");
      this.peon = new HttpLoadQueuePeon(
          "http://" + name + ":8083",
          MAPPER,
          new TestHttpClient(),
          new HttpLoadQueuePeonConfig(null, null, 10),
          () -> SegmentLoadingMode.NORMAL,
          new WrappingScheduledExecutorService(name + "-%s", processingExecutor, true),
          callbackExecutor
      );
      this.peon.start();
    }

    ServerHolder toHolder()
    {
      // Mirrors PrepareBalancerAndLoadQueues: the queue is read against the inventory snapshot it is paired with.
      final ImmutableDruidServer inventory = druidServer.toImmutableDruidServer();
      return new ServerHolder(inventory, peon, peon.getQueueSnapshot(inventory), false, false, 0, 1);
    }

    /**
     * Acknowledges everything queued without running the callbacks, modelling a lagging callback executor.
     */
    void ackServerOnly()
    {
      processingExecutor.finishAllPendingTasks();
    }

    /**
     * Acknowledges everything queued and runs the callbacks. The inventory is deliberately left untouched.
     */
    void ackPendingRequests()
    {
      processingExecutor.finishAllPendingTasks();
      callbackExecutor.finishAllPendingTasks();
    }

    /**
     * Advances the inventory view the way a segment sync would. The peon finds out on the next {@link #toHolder()}.
     */
    void confirmInventory(boolean nowLoaded)
    {
      if (nowLoaded) {
        druidServer.addDataSegment(segment);
      } else {
        druidServer.removeDataSegment(segment.getId());
      }
    }

    @Override
    public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
    {
      segmentsSentToServer.add(segment);
    }

    @Override
    public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
    {
      segmentsSentToServer.add(segment);
    }

    private class TestHttpClient implements HttpClient
    {
      @Override
      public <Intermediate, Final> ListenableFuture<Final> go(
          Request request,
          HttpResponseHandler<Intermediate, Final> handler
      )
      {
        throw new UnsupportedOperationException("Not Implemented.");
      }

      @Override
      @SuppressWarnings("unchecked")
      public <Intermediate, Final> ListenableFuture<Final> go(
          Request request,
          HttpResponseHandler<Intermediate, Final> handler,
          Duration duration
      )
      {
        final HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        httpResponse.setContent(ChannelBuffers.buffer(0));
        handler.handleResponse(httpResponse, null);

        try {
          if (request.getUrl().toString().contains("/loadCapabilities")) {
            return (ListenableFuture<Final>) Futures.immediateFuture(
                new ByteArrayInputStream(
                    MAPPER.writerFor(SegmentLoadingCapabilities.class)
                          .writeValueAsBytes(new SegmentLoadingCapabilities(1, 3))
                )
            );
          }

          final List<DataSegmentChangeRequest> changeRequests = MAPPER.readValue(
              request.getContent().array(),
              HttpLoadQueuePeon.REQUEST_ENTITY_TYPE_REF
          );

          final List<DataSegmentChangeResponse> statuses = new ArrayList<>(changeRequests.size());
          for (DataSegmentChangeRequest cr : changeRequests) {
            cr.go(TestServer.this, null);
            statuses.add(new DataSegmentChangeResponse(cr, SegmentChangeStatus.success()));
          }

          return (ListenableFuture<Final>) Futures.immediateFuture(
              new ByteArrayInputStream(
                  MAPPER.writerFor(HttpLoadQueuePeon.RESPONSE_ENTITY_TYPE_REF).writeValueAsBytes(statuses)
              )
          );
        }
        catch (Exception ex) {
          throw new RE(ex, "Unexpected exception.");
        }
      }
    }
  }
}
