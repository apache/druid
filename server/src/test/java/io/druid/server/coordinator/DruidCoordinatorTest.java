/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.SingleServerInventoryView;
import io.druid.common.config.JacksonConfigManager;
import io.druid.curator.CuratorTestBase;
import io.druid.curator.discovery.NoopServiceAnnouncer;
import io.druid.discovery.DruidLeaderSelector;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.emitter.core.Event;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.IntervalLoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.lookup.cache.LookupCoordinatorManager;
import io.druid.timeline.DataSegment;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidCoordinatorTest extends CuratorTestBase
{
  private DruidCoordinator coordinator;
  private MetadataSegmentManager databaseSegmentManager;
  private SingleServerInventoryView serverInventoryView;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private DruidServer druidServer;
  private DruidServer druidServer2;
  private DataSegment segment;
  private ConcurrentMap<String, LoadQueuePeon> loadManagementPeons;
  private LoadQueuePeon loadQueuePeon;
  private MetadataRuleManager metadataRuleManager;
  private CountDownLatch leaderAnnouncerLatch;
  private CountDownLatch leaderUnannouncerLatch;
  private PathChildrenCache pathChildrenCache;
  private DruidCoordinatorConfig druidCoordinatorConfig;
  private ObjectMapper objectMapper;
  private JacksonConfigManager configManager;
  private DruidNode druidNode;
  private LatchableServiceEmitter serviceEmitter = new LatchableServiceEmitter();
  private static final String LOADPATH = "/druid/loadqueue/localhost:1234";
  private static final long COORDINATOR_START_DELAY = 1;
  private static final long COORDINATOR_PERIOD = 100;

  @Before
  public void setUp() throws Exception
  {
    druidServer = EasyMock.createMock(DruidServer.class);
    serverInventoryView = EasyMock.createMock(SingleServerInventoryView.class);
    databaseSegmentManager = EasyMock.createNiceMock(MetadataSegmentManager.class);
    metadataRuleManager = EasyMock.createNiceMock(MetadataRuleManager.class);
    configManager = EasyMock.createNiceMock(JacksonConfigManager.class);
    EasyMock.expect(
        configManager.watch(
            EasyMock.eq(CoordinatorDynamicConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference(CoordinatorDynamicConfig.builder().build())).anyTimes();
    EasyMock.expect(
        configManager.watch(
            EasyMock.eq(CoordinatorCompactionConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference(CoordinatorCompactionConfig.empty())).anyTimes();
    EasyMock.replay(configManager);
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    curator.create().creatingParentsIfNeeded().forPath(LOADPATH);
    objectMapper = new DefaultObjectMapper();
    druidCoordinatorConfig = new TestDruidCoordinatorConfig(
        new Duration(COORDINATOR_START_DELAY),
        new Duration(COORDINATOR_PERIOD),
        null,
        null,
        new Duration(COORDINATOR_PERIOD),
        null,
        10,
        null,
        false,
        false,
        new Duration("PT0s")
    );
    pathChildrenCache = new PathChildrenCache(
        curator,
        LOADPATH,
        true,
        true,
        Execs.singleThreaded("coordinator_test_path_children_cache-%d")
    );
    loadQueuePeon = new CuratorLoadQueuePeon(
        curator,
        LOADPATH,
        objectMapper,
        Execs.scheduledSingleThreaded("coordinator_test_load_queue_peon_scheduled-%d"),
        Execs.singleThreaded("coordinator_test_load_queue_peon-%d"),
        druidCoordinatorConfig
    );
    loadQueuePeon.start();
    druidNode = new DruidNode("hey", "what", 1234, null, true, false);
    loadManagementPeons = new ConcurrentHashMap<>();
    scheduledExecutorFactory = new ScheduledExecutorFactory()
    {
      @Override
      public ScheduledExecutorService create(int corePoolSize, final String nameFormat)
      {
        return Executors.newSingleThreadScheduledExecutor();
      }
    };
    leaderAnnouncerLatch = new CountDownLatch(1);
    leaderUnannouncerLatch = new CountDownLatch(1);
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        new ZkPathsConfig()
        {

          @Override
          public String getBase()
          {
            return "druid";
          }
        },
        configManager,
        databaseSegmentManager,
        serverInventoryView,
        metadataRuleManager,
        curator,
        serviceEmitter,
        scheduledExecutorFactory,
        null,
        null,
        new NoopServiceAnnouncer()
        {
          @Override
          public void announce(DruidNode node)
          {
            // count down when this coordinator becomes the leader
            leaderAnnouncerLatch.countDown();
          }

          @Override
          public void unannounce(DruidNode node)
          {
            leaderUnannouncerLatch.countDown();
          }
        },
        druidNode,
        loadManagementPeons,
        null,
        new CostBalancerStrategyFactory(),
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector()
    );
  }

  @After
  public void tearDown() throws Exception
  {
    loadQueuePeon.stop();
    pathChildrenCache.close();
    tearDownServerAndCurator();
  }

  @Test
  public void testMoveSegment()
  {
    segment = EasyMock.createNiceMock(DataSegment.class);
    EasyMock.expect(segment.getIdentifier()).andReturn("dummySegment");
    EasyMock.expect(segment.getDataSource()).andReturn("dummyDataSource");
    EasyMock.replay(segment);

    loadQueuePeon = EasyMock.createNiceMock(LoadQueuePeon.class);
    EasyMock.expect(loadQueuePeon.getLoadQueueSize()).andReturn(new Long(1));
    loadQueuePeon.markSegmentToDrop(segment);
    EasyMock.expectLastCall().once();
    Capture<LoadPeonCallback> loadCallbackCapture = Capture.newInstance();
    Capture<LoadPeonCallback> dropCallbackCapture = Capture.newInstance();
    loadQueuePeon.loadSegment(EasyMock.anyObject(DataSegment.class), EasyMock.capture(loadCallbackCapture));
    EasyMock.expectLastCall().once();
    loadQueuePeon.dropSegment(EasyMock.anyObject(DataSegment.class), EasyMock.capture(dropCallbackCapture));
    EasyMock.expectLastCall().once();
    loadQueuePeon.unmarkSegmentToDrop(segment);
    EasyMock.expectLastCall().once();
    EasyMock.expect(loadQueuePeon.getSegmentsToDrop()).andReturn(new HashSet<>()).once();
    EasyMock.replay(loadQueuePeon);

    ImmutableDruidDataSource druidDataSource = EasyMock.createNiceMock(ImmutableDruidDataSource.class);
    EasyMock.expect(druidDataSource.getSegment(EasyMock.anyString())).andReturn(segment);
    EasyMock.replay(druidDataSource);
    EasyMock.expect(databaseSegmentManager.getInventoryValue(EasyMock.anyString())).andReturn(druidDataSource);
    EasyMock.replay(databaseSegmentManager);
    scheduledExecutorFactory = EasyMock.createNiceMock(ScheduledExecutorFactory.class);
    EasyMock.replay(scheduledExecutorFactory);
    EasyMock.replay(metadataRuleManager);
    EasyMock.expect(druidServer.toImmutableDruidServer()).andReturn(
        new ImmutableDruidServer(
            new DruidServerMetadata("from", null, null, 5L, ServerType.HISTORICAL, null, 0),
            1L,
            null,
            ImmutableMap.of("dummySegment", segment)
        )
    ).atLeastOnce();
    EasyMock.replay(druidServer);

    druidServer2 = EasyMock.createMock(DruidServer.class);
    EasyMock.expect(druidServer2.toImmutableDruidServer()).andReturn(
        new ImmutableDruidServer(
            new DruidServerMetadata("to", null, null, 5L, ServerType.HISTORICAL, null, 0),
            1L,
            null,
            ImmutableMap.of("dummySegment2", segment)
        )
    ).atLeastOnce();
    EasyMock.replay(druidServer2);

    loadManagementPeons.put("from", loadQueuePeon);
    loadManagementPeons.put("to", loadQueuePeon);

    EasyMock.expect(serverInventoryView.isSegmentLoadedByServer("to", segment)).andReturn(true).once();
    EasyMock.replay(serverInventoryView);

    coordinator.moveSegment(
        druidServer.toImmutableDruidServer(),
        druidServer2.toImmutableDruidServer(),
        segment,
        null
    );

    LoadPeonCallback loadCallback = loadCallbackCapture.getValue();
    loadCallback.execute();

    LoadPeonCallback dropCallback = dropCallbackCapture.getValue();
    dropCallback.execute();

    EasyMock.verify(druidServer);
    EasyMock.verify(druidServer2);
    EasyMock.verify(loadQueuePeon);
    EasyMock.verify(serverInventoryView);
    EasyMock.verify(metadataRuleManager);
  }

  @Test(timeout = 60_000L)
  public void testCoordinatorRun() throws Exception
  {
    String dataSource = "dataSource1";
    String tier = "hot";

    // Setup MetadataRuleManager
    Rule foreverLoadRule = new ForeverLoadRule(ImmutableMap.of(tier, 2));
    EasyMock.expect(metadataRuleManager.getRulesWithDefault(EasyMock.anyString()))
            .andReturn(ImmutableList.of(foreverLoadRule)).atLeastOnce();

    metadataRuleManager.stop();
    EasyMock.expectLastCall().once();

    EasyMock.replay(metadataRuleManager);

    // Setup MetadataSegmentManager
    DruidDataSource[] druidDataSources = {
        new DruidDataSource(dataSource, Collections.<String, String>emptyMap())
    };
    final DataSegment dataSegment = new DataSegment(
        dataSource,
        Intervals.of("2010-01-01/P1D"),
        "v1",
        null,
        null,
        null,
        null,
        0x9,
        0
    );
    druidDataSources[0].addSegment(dataSegment);

    EasyMock.expect(databaseSegmentManager.isStarted()).andReturn(true).anyTimes();
    EasyMock.expect(databaseSegmentManager.getInventory()).andReturn(
        ImmutableList.of(druidDataSources[0].toImmutableDruidDataSource())
    ).atLeastOnce();
    EasyMock.replay(databaseSegmentManager);
    ImmutableDruidDataSource immutableDruidDataSource = EasyMock.createNiceMock(ImmutableDruidDataSource.class);
    EasyMock.expect(immutableDruidDataSource.getSegments())
            .andReturn(ImmutableSet.of(dataSegment)).atLeastOnce();
    EasyMock.replay(immutableDruidDataSource);

    // Setup ServerInventoryView
    druidServer = new DruidServer("server1", "localhost", null, 5L, ServerType.HISTORICAL, tier, 0);
    loadManagementPeons.put("server1", loadQueuePeon);
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(
        ImmutableList.of(druidServer)
    ).atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();
    EasyMock.replay(serverInventoryView);

    coordinator.start();
    // Wait for this coordinator to become leader
    leaderAnnouncerLatch.await();

    // This coordinator should be leader by now
    Assert.assertTrue(coordinator.isLeader());
    Assert.assertEquals(druidNode.getHostAndPort(), coordinator.getCurrentLeader());

    final CountDownLatch assignSegmentLatch = new CountDownLatch(1);
    pathChildrenCache.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(
              CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent
          )
          {
            if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
              if (assignSegmentLatch.getCount() > 0) {
                //Coordinator should try to assign segment to druidServer historical
                //Simulate historical loading segment
                druidServer.addDataSegment(dataSegment);
                assignSegmentLatch.countDown();
              } else {
                Assert.fail("The same segment is assigned to the same server multiple times");
              }
            }
          }
        }
    );
    pathChildrenCache.start();

    assignSegmentLatch.await();

    final CountDownLatch coordinatorRunLatch = new CountDownLatch(2);
    serviceEmitter.latch = coordinatorRunLatch;
    coordinatorRunLatch.await();

    Assert.assertEquals(ImmutableMap.of(dataSource, 100.0), coordinator.getLoadStatus());
    curator.delete().guaranteed().forPath(ZKPaths.makePath(LOADPATH, dataSegment.getIdentifier()));

    Map segmentAvailability = coordinator.getSegmentAvailability();
    Assert.assertEquals(1, segmentAvailability.size());
    Assert.assertEquals(0L, segmentAvailability.get(dataSource));

    Map<String, ? extends Object2LongMap<String>> replicationStatus = coordinator.getReplicationStatus();
    Assert.assertNotNull(replicationStatus);
    Assert.assertEquals(1, replicationStatus.entrySet().size());

    Object2LongMap<String> dataSourceMap = replicationStatus.get(tier);
    Assert.assertNotNull(dataSourceMap);
    Assert.assertEquals(1, dataSourceMap.size());
    Assert.assertNotNull(dataSourceMap.get(dataSource));
    // Simulated the adding of segment to druidServer during SegmentChangeRequestLoad event
    // The load rules asks for 2 replicas, therefore 1 replica should still be pending
    Assert.assertEquals(1L, dataSourceMap.getLong(dataSource));

    coordinator.stop();
    leaderUnannouncerLatch.await();

    Assert.assertFalse(coordinator.isLeader());
    Assert.assertNull(coordinator.getCurrentLeader());

    EasyMock.verify(serverInventoryView);
    EasyMock.verify(metadataRuleManager);
  }

  @Test(timeout = 60_000L)
  public void testCoordinatorTieredRun() throws Exception
  {
    final String dataSource = "dataSource", hotTierName = "hot", coldTierName = "cold";
    final Rule hotTier = new IntervalLoadRule(Intervals.of("2018-01-01/P1M"), ImmutableMap.of(hotTierName, 1));
    final Rule coldTier = new ForeverLoadRule(ImmutableMap.of(coldTierName, 1));
    final String loadPathCold = "/druid/loadqueue/cold:1234";
    final DruidServer hotServer = new DruidServer("hot", "hot", null, 5L, ServerType.HISTORICAL, hotTierName, 0);
    final DruidServer coldServer = new DruidServer("cold", "cold", null, 5L, ServerType.HISTORICAL, coldTierName, 0);

    final Map<String, DataSegment> dataSegments = ImmutableMap.of(
        "2018-01-02T00:00:00.000Z_2018-01-03T00:00:00.000Z",
        new DataSegment(dataSource, Intervals.of("2018-01-02/P1D"), "v1", null, null, null, null, 0x9, 0),
        "2018-01-03T00:00:00.000Z_2018-01-04T00:00:00.000Z",
        new DataSegment(dataSource, Intervals.of("2018-01-03/P1D"), "v1", null, null, null, null, 0x9, 0),
        "2017-01-01T00:00:00.000Z_2017-01-02T00:00:00.000Z",
        new DataSegment(dataSource, Intervals.of("2017-01-01/P1D"), "v1", null, null, null, null, 0x9, 0)
    );

    final LoadQueuePeon loadQueuePeonCold = new CuratorLoadQueuePeon(
        curator,
        loadPathCold,
        objectMapper,
        Execs.scheduledSingleThreaded("coordinator_test_load_queue_peon_cold_scheduled-%d"),
        Execs.singleThreaded("coordinator_test_load_queue_peon_cold-%d"),
        druidCoordinatorConfig
    );
    final PathChildrenCache pathChildrenCacheCold = new PathChildrenCache(
        curator, loadPathCold, true, true, Execs.singleThreaded("coordinator_test_path_children_cache_cold-%d")
    );
    loadManagementPeons.putAll(ImmutableMap.of("hot", loadQueuePeon, "cold", loadQueuePeonCold));

    loadQueuePeonCold.start();
    pathChildrenCache.start();
    pathChildrenCacheCold.start();

    DruidDataSource[] druidDataSources = {new DruidDataSource(dataSource, Collections.emptyMap())};
    dataSegments.values().forEach(druidDataSources[0]::addSegment);

    EasyMock.expect(metadataRuleManager.getRulesWithDefault(EasyMock.anyString()))
            .andReturn(ImmutableList.of(hotTier, coldTier)).atLeastOnce();
    EasyMock.expect(databaseSegmentManager.isStarted()).andReturn(true).anyTimes();
    EasyMock.expect(databaseSegmentManager.getInventory()).andReturn(
        ImmutableList.of(druidDataSources[0].toImmutableDruidDataSource())
    ).atLeastOnce();
    EasyMock.expect(serverInventoryView.getInventory())
            .andReturn(ImmutableList.of(hotServer, coldServer))
            .atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();

    EasyMock.replay(metadataRuleManager, databaseSegmentManager, serverInventoryView);

    coordinator.start();
    leaderAnnouncerLatch.await(); // Wait for this coordinator to become leader

    final CountDownLatch assignSegmentLatchHot = new CountDownLatch(2);
    pathChildrenCache.getListenable().addListener(
        (client, event) -> {
          if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
            DataSegment segment = dataSegments
                .entrySet()
                .stream()
                .filter(x -> event.getData().getPath().contains(x.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);

            if (segment != null) {
              hotServer.addDataSegment(segment);
              curator.delete().guaranteed().forPath(event.getData().getPath());
            }

            assignSegmentLatchHot.countDown();
          }
        }
    );

    final CountDownLatch assignSegmentLatchCold = new CountDownLatch(1);
    pathChildrenCacheCold.getListenable().addListener(
        (client, event) -> {
          if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
            DataSegment segment = dataSegments
                .entrySet()
                .stream()
                .filter(x -> event.getData().getPath().contains(x.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);

            if (segment != null) {
              coldServer.addDataSegment(segment);
              curator.delete().guaranteed().forPath(event.getData().getPath());
            }

            assignSegmentLatchCold.countDown();
          }
        }
    );

    assignSegmentLatchHot.await();
    assignSegmentLatchCold.await();

    final CountDownLatch coordinatorRunLatch = new CountDownLatch(2);
    serviceEmitter.latch = coordinatorRunLatch;
    coordinatorRunLatch.await();

    Assert.assertEquals(ImmutableMap.of(dataSource, 100.0), coordinator.getLoadStatus());

    Map<String, ? extends Object2LongMap<String>> replicationStatus = coordinator.getReplicationStatus();
    Assert.assertEquals(2, replicationStatus.entrySet().size());
    Assert.assertEquals(0L, replicationStatus.get(hotTierName).getLong(dataSource));
    Assert.assertEquals(0L, replicationStatus.get(coldTierName).getLong(dataSource));

    coordinator.stop();
    leaderUnannouncerLatch.await();

    EasyMock.verify(serverInventoryView);
    EasyMock.verify(databaseSegmentManager);
    EasyMock.verify(metadataRuleManager);
  }

  @Test
  public void testOrderedAvailableDataSegments()
  {
    DruidDataSource dataSource = new DruidDataSource("test", new HashMap());
    DataSegment[] segments = new DataSegment[]{
        getSegment("test", Intervals.of("2016-01-10T03:00:00Z/2016-01-10T04:00:00Z")),
        getSegment("test", Intervals.of("2016-01-11T01:00:00Z/2016-01-11T02:00:00Z")),
        getSegment("test", Intervals.of("2016-01-09T10:00:00Z/2016-01-09T11:00:00Z")),
        getSegment("test", Intervals.of("2016-01-09T10:00:00Z/2016-01-09T12:00:00Z"))
    };
    for (DataSegment segment : segments) {
      dataSource.addSegment(segment);
    }

    EasyMock.expect(databaseSegmentManager.getInventory()).andReturn(
        ImmutableList.of(dataSource.toImmutableDruidDataSource())
    ).atLeastOnce();
    EasyMock.replay(databaseSegmentManager);
    Set<DataSegment> availableSegments = coordinator.getOrderedAvailableDataSegments();
    DataSegment[] expected = new DataSegment[]{
        getSegment("test", Intervals.of("2016-01-11T01:00:00Z/2016-01-11T02:00:00Z")),
        getSegment("test", Intervals.of("2016-01-10T03:00:00Z/2016-01-10T04:00:00Z")),
        getSegment("test", Intervals.of("2016-01-09T10:00:00Z/2016-01-09T12:00:00Z")),
        getSegment("test", Intervals.of("2016-01-09T10:00:00Z/2016-01-09T11:00:00Z"))
    };
    Assert.assertEquals(expected.length, availableSegments.size());
    Assert.assertEquals(expected, availableSegments.toArray());
    EasyMock.verify(databaseSegmentManager);
  }


  private DataSegment getSegment(String dataSource, Interval interval)
  {
    // Not using EasyMock as it hampers the performance of multithreads.
    DataSegment segment = new DataSegment(
        dataSource, interval, "dummy_version", new ConcurrentHashMap<>(),
        Lists.<String>newArrayList(), Lists.<String>newArrayList(), null, 0, 0L
    );
    return segment;
  }

  private static class TestDruidLeaderSelector implements DruidLeaderSelector
  {
    private volatile Listener listener;
    private volatile String leader;

    @Override
    public String getCurrentLeader()
    {
      return leader;
    }

    @Override
    public boolean isLeader()
    {
      return leader != null;
    }

    @Override
    public int localTerm()
    {
      return 0;
    }

    @Override
    public void registerListener(Listener listener)
    {
      this.listener = listener;
      leader = "what:1234";
      listener.becomeLeader();
    }

    @Override
    public void unregisterListener()
    {
      leader = null;
      listener.stopBeingLeader();
    }
  }

  private static class LatchableServiceEmitter extends ServiceEmitter
  {
    private CountDownLatch latch;

    private LatchableServiceEmitter()
    {
      super("", "", null);
    }

    @Override
    public void emit(Event event)
    {
      if (latch != null && "segment/count".equals(event.toMap().get("metric"))) {
        latch.countDown();
      }
    }
  }
}
