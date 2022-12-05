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

package org.apache.druid.sql.calcite.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.client.BrokerInternalQueryConfig;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.FilteringSegmentCallback;
import org.apache.druid.client.ServerView.CallbackAction;
import org.apache.druid.client.ServerView.SegmentCallback;
import org.apache.druid.client.ServerView.ServerRemovedCallback;
import org.apache.druid.client.TimelineServerView.TimelineCallback;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class SegmentDataCacheConcurrencyTest extends SegmentMetadataCacheCommon
{
  private static final String DATASOURCE = "datasource";

  private File tmpDir;
  private SpecificSegmentsQuerySegmentWalker walker;
  private TestServerInventoryView inventoryView;
  private BrokerServerView serverView;
  private SegmentMetadataCache schema;
  private ExecutorService exec;

  @Before
  public void setUp() throws Exception
  {
    tmpDir = temporaryFolder.newFolder();
    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate);
    inventoryView = new TestServerInventoryView();
    serverView = newBrokerServerView(inventoryView);
    inventoryView.init();
    serverView.awaitInitialization();
    exec = Execs.multiThreaded(4, "DruidSchemaConcurrencyTest-%d");
  }

  @After
  public void tearDown() throws Exception
  {
    exec.shutdownNow();
    walker.close();
  }

  /**
   * This tests the contention between three components, {@link SegmentMetadataCache},
   * {@code InventoryView}, and {@link BrokerServerView}. It first triggers
   * refreshing {@code SegmentMetadataCache}. To mimic some heavy work done with
   * {@link SegmentMetadataCache#lock}, {@link SegmentMetadataCache#buildDruidTable}
   * is overridden to sleep before doing real work. While refreshing
   * {@code SegmentMetadataCache}, more new segments are added to
   * {@code InventoryView}, which triggers updates of {@code BrokerServerView}.
   * Finally, while {@code BrokerServerView} is updated,
   * {@link BrokerServerView#getTimeline} is continuously called to mimic user query
   * processing. All these calls must return without heavy contention.
   */
  @Test(timeout = 30000L)
  public void testSegmentMetadataRefreshAndInventoryViewAddSegmentAndBrokerServerViewGetTimeline()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig()
    )
    {
      @Override
      DatasourceTable.PhysicalDatasourceMetadata buildDruidTable(final String dataSource)
      {
        doInLock(() -> {
          try {
            // Mimic some heavy work done in lock in DruidSchema
            Thread.sleep(5000);
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
        return super.buildDruidTable(dataSource);
      }
    };

    int numExistingSegments = 100;
    int numServers = 19;
    CountDownLatch segmentLoadLatch = new CountDownLatch(numExistingSegments);
    serverView.registerTimelineCallback(
        Execs.directExecutor(),
        new TimelineCallback()
        {
          @Override
          public CallbackAction timelineInitialized()
          {
            return CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            segmentLoadLatch.countDown();
            return CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction segmentRemoved(DataSegment segment)
          {
            return CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction serverSegmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            return CallbackAction.CONTINUE;
          }
        }
    );
    addSegmentsToCluster(0, numServers, numExistingSegments);
    // Wait for all segments to be loaded in BrokerServerView
    Assert.assertTrue(segmentLoadLatch.await(5, TimeUnit.SECONDS));

    // Trigger refresh of DruidSchema. This will internally run the heavy work
    // mimicked by the overridden buildDruidTable
    Future<?> refreshFuture = exec.submit(() -> {
      schema.refresh(
          walker.getSegments().stream().map(DataSegment::getId).collect(Collectors.toSet()),
          Sets.newHashSet(DATASOURCE)
      );
      return null;
    });

    // Trigger updates of BrokerServerView. This should be done asynchronously.
    addSegmentsToCluster(numExistingSegments, numServers, 50); // add completely new segments
    addReplicasToCluster(1, numServers, 30); // add replicas of the first 30 segments.
    // for the first 30 segments, we will still have replicas.
    // for the other 20 segments, they will be completely removed from the cluster.
    removeSegmentsFromCluster(numServers, 50);
    Assert.assertFalse(refreshFuture.isDone());

    for (int i = 0; i < 1000; i++) {
      boolean hasTimeline = exec.submit(
          () -> serverView.getTimeline(DataSourceAnalysis.forDataSource(new TableDataSource(DATASOURCE)))
                          .isPresent()
      ).get(100, TimeUnit.MILLISECONDS);
      Assert.assertTrue(hasTimeline);
      // We want to call getTimeline while BrokerServerView is being updated. Sleep might help with timing.
      Thread.sleep(2);
    }

    refreshFuture.get(10, TimeUnit.SECONDS);
  }

  /**
   * This tests the contention between two methods of {@link SegmentMetadataCache}:
   * {@link SegmentMetadataCache#refresh} and
   * {@link SegmentMetadataCache#getSegmentMetadataSnapshot()}. It first triggers
   * refreshing {@code SegmentMetadataCache}. To mimic some heavy work done with
   * {@link SegmentMetadataCache#lock}, {@link SegmentMetadataCache#buildDruidTable}
   * is overridden to sleep before doing real work. While refreshing
   * {@code SegmentMetadataCache}, {@code getSegmentMetadataSnapshot()} is continuously
   * called to mimic reading the segments table of SystemSchema. All these calls
   * must return without heavy contention.
   */
  @Test(timeout = 30000L)
  public void testSegmentMetadataRefreshAndDruidSchemaGetSegmentMetadata()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig()
    )
    {
      @Override
      DatasourceTable.PhysicalDatasourceMetadata buildDruidTable(final String dataSource)
      {
        doInLock(() -> {
          try {
            // Mimic some heavy work done in lock in SegmentMetadataCache
            Thread.sleep(5000);
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
        return super.buildDruidTable(dataSource);
      }
    };

    int numExistingSegments = 100;
    int numServers = 19;
    CountDownLatch segmentLoadLatch = new CountDownLatch(numExistingSegments);
    serverView.registerTimelineCallback(
        Execs.directExecutor(),
        new TimelineCallback()
        {
          @Override
          public CallbackAction timelineInitialized()
          {
            return CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            segmentLoadLatch.countDown();
            return CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction segmentRemoved(DataSegment segment)
          {
            return CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction serverSegmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            return CallbackAction.CONTINUE;
          }
        }
    );
    addSegmentsToCluster(0, numServers, numExistingSegments);
    // Wait for all segments to be loaded in BrokerServerView
    Assert.assertTrue(segmentLoadLatch.await(5, TimeUnit.SECONDS));

    // Trigger refresh of SegmentMetadataCache. This will internally run the heavy work mimicked
    // by the overridden buildDruidTable
    Future<?> refreshFuture = exec.submit(() -> {
      schema.refresh(
          walker.getSegments().stream().map(DataSegment::getId).collect(Collectors.toSet()),
          Sets.newHashSet(DATASOURCE)
      );
      return null;
    });
    Assert.assertFalse(refreshFuture.isDone());

    for (int i = 0; i < 1000; i++) {
      Map<SegmentId, AvailableSegmentMetadata> segmentsMetadata = exec.submit(
          () -> schema.getSegmentMetadataSnapshot()
      ).get(100, TimeUnit.MILLISECONDS);
      Assert.assertFalse(segmentsMetadata.isEmpty());
      // We want to call getTimeline while refreshing. Sleep might help with timing.
      Thread.sleep(2);
    }

    refreshFuture.get(10, TimeUnit.SECONDS);
  }

  private void addSegmentsToCluster(int partitionIdStart, int numServers, int numSegments)
  {
    for (int i = 0; i < numSegments; i++) {
      DataSegment segment = newSegment(i + partitionIdStart);
      QueryableIndex index = newQueryableIndex(i + partitionIdStart);
      walker.add(segment, index);
      int serverIndex = i % numServers;
      inventoryView.addServerSegment(newServer("server_" + serverIndex), segment);
    }
  }

  private void addReplicasToCluster(int serverIndexOffFrom, int numServers, int numSegments)
  {
    for (int i = 0; i < numSegments; i++) {
      DataSegment segment = newSegment(i);
      int serverIndex = i % numServers + serverIndexOffFrom;
      serverIndex = serverIndex < numServers ? serverIndex : serverIndex - numServers;
      inventoryView.addServerSegment(newServer("server_" + serverIndex), segment);
    }
  }

  private void removeSegmentsFromCluster(int numServers, int numSegments)
  {
    for (int i = 0; i < numSegments; i++) {
      DataSegment segment = newSegment(i);
      int serverIndex = i % numServers;
      inventoryView.removeServerSegment(newServer("server_" + serverIndex), segment);
    }
  }

  private static BrokerServerView newBrokerServerView(FilteredServerInventoryView baseView)
  {
    return new BrokerServerView(
        EasyMock.createMock(QueryToolChestWarehouse.class),
        EasyMock.createMock(QueryWatcher.class),
        new DefaultObjectMapper(),
        EasyMock.createMock(HttpClient.class),
        baseView,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        new NoopServiceEmitter(),
        new BrokerSegmentWatcherConfig()
    );
  }

  private static DruidServer newServer(String name)
  {
    return new DruidServer(
        name,
        "host:8083",
        "host:8283",
        1000L,
        ServerType.HISTORICAL,
        "tier",
        0
    );
  }

  private DataSegment newSegment(int partitionId)
  {
    return new DataSegment(
        DATASOURCE,
        Intervals.of("2012/2013"),
        "version1",
        null,
        ImmutableList.of(),
        ImmutableList.of(),
        new NumberedShardSpec(partitionId, 0),
        null,
        1,
        100L,
        PruneSpecsHolder.DEFAULT
    );
  }

  private QueryableIndex newQueryableIndex(int partitionId)
  {
    return IndexBuilder.create()
                       .tmpDir(new File(tmpDir, "" + partitionId))
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(
                           new IncrementalIndexSchema.Builder()
                               .withMetrics(
                                   new CountAggregatorFactory("cnt"),
                                   new DoubleSumAggregatorFactory("m1", "m1")
                               )
                               .withRollup(false)
                               .build()
                       )
                       .rows(ROWS1)
                       .buildMMappedIndex();
  }

  private static class TestServerInventoryView implements FilteredServerInventoryView
  {
    private final Map<String, DruidServer> serverMap = new HashMap<>();
    private final Map<String, Set<DataSegment>> segmentsMap = new HashMap<>();
    private final List<NonnullPair<SegmentCallback, Executor>> segmentCallbacks = new ArrayList<>();
    private final List<NonnullPair<ServerRemovedCallback, Executor>> serverRemovedCallbacks = new ArrayList<>();

    private void init()
    {
      segmentCallbacks.forEach(pair -> pair.rhs.execute(pair.lhs::segmentViewInitialized));
    }

    private void addServerSegment(DruidServer server, DataSegment segment)
    {
      serverMap.put(server.getName(), server);
      segmentsMap.computeIfAbsent(server.getName(), k -> new HashSet<>()).add(segment);
      segmentCallbacks.forEach(pair -> pair.rhs.execute(() -> pair.lhs.segmentAdded(server.getMetadata(), segment)));
    }

    private void removeServerSegment(DruidServer server, DataSegment segment)
    {
      segmentsMap.computeIfAbsent(server.getName(), k -> new HashSet<>()).remove(segment);
      segmentCallbacks.forEach(pair -> pair.rhs.execute(() -> pair.lhs.segmentRemoved(server.getMetadata(), segment)));
    }

    @SuppressWarnings("unused")
    private void removeServer(DruidServer server)
    {
      serverMap.remove(server.getName());
      segmentsMap.remove(server.getName());
      serverRemovedCallbacks.forEach(pair -> pair.rhs.execute(() -> pair.lhs.serverRemoved(server)));
    }

    @Override
    public void registerSegmentCallback(
        Executor exec,
        SegmentCallback callback,
        Predicate<Pair<DruidServerMetadata, DataSegment>> filter
    )
    {
      segmentCallbacks.add(new NonnullPair<>(new FilteringSegmentCallback(callback, filter), exec));
    }

    @Override
    public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
    {
      serverRemovedCallbacks.add(new NonnullPair<>(callback, exec));
    }

    @Nullable
    @Override
    public DruidServer getInventoryValue(String serverKey)
    {
      return serverMap.get(serverKey);
    }

    @Override
    public Collection<DruidServer> getInventory()
    {
      return serverMap.values();
    }

    @Override
    public boolean isStarted()
    {
      return true;
    }

    @Override
    public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
    {
      Set<DataSegment> segments = segmentsMap.get(serverKey);
      return segments != null && segments.contains(segment);
    }
  }
}
