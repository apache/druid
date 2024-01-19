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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.client.BatchServerInventoryView;
import org.apache.druid.client.CoordinatorSegmentWatcherConfig;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.curator.CuratorUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.loading.CuratorLoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.SegmentLoadingConfig;
import org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.testing.DeadlockDetectingTimeout;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This tests zookeeper specific coordinator/load queue/historical interactions, such as moving segments by the balancer
 */
public class CuratorDruidCoordinatorTest extends CuratorTestBase
{
  private DataSourcesSnapshot dataSourcesSnapshot;
  private DruidCoordinatorRuntimeParams coordinatorRuntimeParams;

  private LoadQueuePeon sourceLoadQueuePeon;
  private LoadQueuePeon destinationLoadQueuePeon;
  private PathChildrenCache sourceLoadQueueChildrenCache;
  private PathChildrenCache destinationLoadQueueChildrenCache;

  private static final String SEGPATH = "/druid/segments";
  private static final String SOURCE_LOAD_PATH = "/druid/loadQueue/localhost:1";
  private static final String DESTINATION_LOAD_PATH = "/druid/loadQueue/localhost:2";
  private static final long COORDINATOR_START_DELAY = 1;
  private static final long COORDINATOR_PERIOD = 100;

  private BatchServerInventoryView baseView;
  private CoordinatorServerView serverView;
  private CountDownLatch segmentViewInitLatch;
  /**
   * The following two fields are changed during {@link #testMoveSegment()}, the change might not be visible from the
   * thread, that runs the callback, registered in {@link #setupView()}. volatile modificator doesn't guarantee
   * visibility either, but somewhat increases the chances.
   */
  private volatile CountDownLatch segmentAddedLatch;
  private volatile CountDownLatch segmentRemovedLatch;
  private final ObjectMapper jsonMapper;
  private final ZkPathsConfig zkPathsConfig;

  private final ScheduledExecutorService peonExec = Execs.scheduledSingleThreaded("Master-PeonExec--%d");
  private final ExecutorService callbackExec = Execs.multiThreaded(4, "LoadQueuePeon-callbackexec--%d");

  public CuratorDruidCoordinatorTest()
  {
    jsonMapper = TestHelper.makeJsonMapper();
    zkPathsConfig = new ZkPathsConfig();
  }

  @Before
  public void setUp() throws Exception
  {
    dataSourcesSnapshot = EasyMock.createNiceMock(DataSourcesSnapshot.class);
    coordinatorRuntimeParams = EasyMock.createNiceMock(DruidCoordinatorRuntimeParams.class);

    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    curator.create().creatingParentsIfNeeded().forPath(SEGPATH);
    curator.create().creatingParentsIfNeeded().forPath(SOURCE_LOAD_PATH);
    curator.create().creatingParentsIfNeeded().forPath(DESTINATION_LOAD_PATH);

    final ObjectMapper objectMapper = new DefaultObjectMapper();
    DruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withCoordinatorStartDelay(new Duration(COORDINATOR_START_DELAY))
        .withCoordinatorPeriod(new Duration(COORDINATOR_PERIOD))
        .withCoordinatorKillPeriod(new Duration(COORDINATOR_PERIOD))
        .withCoordinatorKillMaxSegments(10)
        .withCoordinatorKillIgnoreDurationToRetain(false)
        .build();
    sourceLoadQueueChildrenCache = new PathChildrenCache(
        curator,
        SOURCE_LOAD_PATH,
        true,
        true,
        Execs.singleThreaded("coordinator_test_path_children_cache_src-%d")
    );
    destinationLoadQueueChildrenCache = new PathChildrenCache(
        curator,
        DESTINATION_LOAD_PATH,
        true,
        true,
        Execs.singleThreaded("coordinator_test_path_children_cache_dest-%d")
    );
    sourceLoadQueuePeon = new CuratorLoadQueuePeon(
        curator,
        SOURCE_LOAD_PATH,
        objectMapper,
        peonExec,
        callbackExec,
        druidCoordinatorConfig
    );
    destinationLoadQueuePeon = new CuratorLoadQueuePeon(
        curator,
        DESTINATION_LOAD_PATH,
        objectMapper,
        peonExec,
        callbackExec,
        druidCoordinatorConfig
    );
  }

  @After
  public void tearDown() throws Exception
  {
    baseView.stop();
    sourceLoadQueuePeon.stop();
    sourceLoadQueueChildrenCache.close();
    destinationLoadQueueChildrenCache.close();
    tearDownServerAndCurator();
  }

  @Rule
  public final TestRule timeout = new DeadlockDetectingTimeout(60, TimeUnit.SECONDS);

  @Test
  public void testStopDoesntKillPoolItDoesntOwn() throws Exception
  {
    setupView();
    sourceLoadQueuePeon.stop();
    Assert.assertFalse(peonExec.isShutdown());
    Assert.assertFalse(callbackExec.isShutdown());
  }

  @Test
  public void testMoveSegment() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(4);

    segmentRemovedLatch = new CountDownLatch(0);

    setupView();

    DruidServer source = new DruidServer(
        "localhost:1",
        "localhost:1",
        null,
        10000000L,
        ServerType.HISTORICAL,
        "default_tier",
        0
    );

    DruidServer dest = new DruidServer(
        "localhost:2",
        "localhost:2",
        null,
        10000000L,
        ServerType.HISTORICAL,
        "default_tier",
        0
    );

    setupZNodeForServer(source, zkPathsConfig, jsonMapper);
    setupZNodeForServer(dest, zkPathsConfig, jsonMapper);

    final List<DataSegment> sourceSegments = Arrays.asList(
        createSegment("2011-04-01/2011-04-03", "v1"),
        createSegment("2011-04-03/2011-04-06", "v1"),
        createSegment("2011-04-06/2011-04-09", "v1")
    );

    final List<DataSegment> destinationSegments = Collections.singletonList(
        createSegment("2011-03-31/2011-04-01", "v1")
    );

    DataSegment segmentToMove = sourceSegments.get(2);

    List<String> sourceSegKeys = new ArrayList<>();

    for (DataSegment segment : sourceSegments) {
      sourceSegKeys.add(announceBatchSegmentsForServer(source, ImmutableSet.of(segment), zkPathsConfig, jsonMapper));
    }

    for (DataSegment segment : destinationSegments) {
      announceBatchSegmentsForServer(dest, ImmutableSet.of(segment), zkPathsConfig, jsonMapper);
    }

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    // these child watchers are used to simulate actions of historicals, announcing a segment on noticing a load queue
    // for the destination and unannouncing from source server when noticing a drop request

    CountDownLatch srcCountdown = new CountDownLatch(1);
    sourceLoadQueueChildrenCache.getListenable().addListener(
        (CuratorFramework curatorFramework, PathChildrenCacheEvent event) -> {
          if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
            srcCountdown.countDown();
          } else if (CuratorUtils.isChildAdded(event)) {
            //Simulate source server dropping segment
            unannounceSegmentFromBatchForServer(source, segmentToMove, sourceSegKeys.get(2), zkPathsConfig);
          }
        }
    );

    CountDownLatch destCountdown = new CountDownLatch(1);
    destinationLoadQueueChildrenCache.getListenable().addListener(
        (CuratorFramework curatorFramework, PathChildrenCacheEvent event) -> {
          if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
            destCountdown.countDown();
          } else if (CuratorUtils.isChildAdded(event)) {
            //Simulate destination server loading segment
            announceBatchSegmentsForServer(dest, ImmutableSet.of(segmentToMove), zkPathsConfig, jsonMapper);
          }
        }
    );

    sourceLoadQueueChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
    destinationLoadQueueChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

    Assert.assertTrue(timing.forWaiting().awaitLatch(srcCountdown));
    Assert.assertTrue(timing.forWaiting().awaitLatch(destCountdown));

    sourceSegments.forEach(source::addDataSegment);
    destinationSegments.forEach(dest::addDataSegment);

    segmentRemovedLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);

    ImmutableDruidDataSource druidDataSource = EasyMock.createNiceMock(ImmutableDruidDataSource.class);
    EasyMock.expect(druidDataSource.getSegment(EasyMock.anyObject(SegmentId.class))).andReturn(sourceSegments.get(2));
    EasyMock.replay(druidDataSource);
    EasyMock.expect(coordinatorRuntimeParams.getDataSourcesSnapshot())
            .andReturn(dataSourcesSnapshot).anyTimes();
    final CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder().withUseRoundRobinSegmentAssignment(false).build();
    EasyMock.expect(coordinatorRuntimeParams.getCoordinatorDynamicConfig())
            .andReturn(dynamicConfig)
            .anyTimes();
    EasyMock.expect(coordinatorRuntimeParams.getSegmentLoadingConfig())
            .andReturn(SegmentLoadingConfig.create(dynamicConfig, 100))
            .anyTimes();

    final ServerHolder sourceServer = new ServerHolder(source.toImmutableDruidServer(), sourceLoadQueuePeon);
    final ServerHolder destinationServer = new ServerHolder(dest.toImmutableDruidServer(), destinationLoadQueuePeon);
    final DruidCluster cluster = DruidCluster.builder().add(sourceServer).add(destinationServer).build();

    final BalancerStrategy balancerStrategy = EasyMock.mock(BalancerStrategy.class);
    EasyMock.expect(
        balancerStrategy.findDestinationServerToMoveSegment(
            EasyMock.anyObject(),
            EasyMock.anyObject(),
            EasyMock.anyObject()
        )
    ).andReturn(destinationServer).atLeastOnce();
    EasyMock.expect(coordinatorRuntimeParams.getBalancerStrategy())
            .andReturn(balancerStrategy).anyTimes();
    EasyMock.expect(coordinatorRuntimeParams.getDruidCluster()).andReturn(cluster).anyTimes();
    EasyMock.replay(coordinatorRuntimeParams, balancerStrategy);

    EasyMock.expect(dataSourcesSnapshot.getDataSource(EasyMock.anyString()))
            .andReturn(druidDataSource).anyTimes();
    EasyMock.replay(dataSourcesSnapshot);

    LoadQueueTaskMaster taskMaster = EasyMock.createMock(LoadQueueTaskMaster.class);
    EasyMock.expect(taskMaster.isHttpLoading()).andReturn(false).anyTimes();
    EasyMock.replay(taskMaster);

    // Move the segment from source to dest
    SegmentLoadQueueManager loadQueueManager =
        new SegmentLoadQueueManager(baseView, taskMaster);
    StrategicSegmentAssigner segmentAssigner = createSegmentAssigner(loadQueueManager, coordinatorRuntimeParams);
    segmentAssigner.moveSegment(
        segmentToMove,
        sourceServer,
        Collections.singletonList(destinationServer)
    );

    // wait for destination server to load segment
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    // remove load queue key from destination server to trigger adding drop to load queue
    curator.delete().guaranteed().forPath(ZKPaths.makePath(DESTINATION_LOAD_PATH, segmentToMove.getId().toString()));

    // wait for drop
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    // clean up drop from load queue
    curator.delete().guaranteed().forPath(ZKPaths.makePath(SOURCE_LOAD_PATH, segmentToMove.getId().toString()));

    List<DruidServer> servers = new ArrayList<>(serverView.getInventory());

    Assert.assertEquals(2, servers.get(0).getTotalSegments());
    Assert.assertEquals(2, servers.get(1).getTotalSegments());
  }

  private void setupView() throws Exception
  {
    baseView = new BatchServerInventoryView(
        zkPathsConfig,
        curator,
        jsonMapper,
        Predicates.alwaysTrue(),
        "test"
    )
    {
      @Override
      public void registerSegmentCallback(Executor exec, final SegmentCallback callback)
      {
        super.registerSegmentCallback(
            exec,
            new SegmentCallback()
            {
              @Override
              public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentAdded(server, segment);
                segmentAddedLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentRemoved(server, segment);
                segmentRemovedLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentViewInitialized()
              {
                CallbackAction res = callback.segmentViewInitialized();
                segmentViewInitLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
              {
                return CallbackAction.CONTINUE;
              }
            }
        );
      }
    };

    serverView = new CoordinatorServerView(baseView, new CoordinatorSegmentWatcherConfig(), new NoopServiceEmitter(), null);

    baseView.start();

    sourceLoadQueuePeon.start();
    destinationLoadQueuePeon.start();
  }

  private DataSegment createSegment(String intervalStr, String version)
  {
    return DataSegment.builder()
                      .dataSource("test_curator_druid_coordinator")
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(ImmutableMap.of("type", "local", "path", "somewhere"))
                      .version(version)
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }

  private StrategicSegmentAssigner createSegmentAssigner(
      SegmentLoadQueueManager loadQueueManager,
      DruidCoordinatorRuntimeParams params
  )
  {
    return new StrategicSegmentAssigner(
        loadQueueManager,
        params.getDruidCluster(),
        params.getBalancerStrategy(),
        params.getSegmentLoadingConfig(),
        new CoordinatorRunStats()
    );
  }
}
