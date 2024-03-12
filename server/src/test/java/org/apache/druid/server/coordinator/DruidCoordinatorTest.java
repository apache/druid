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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.druid.client.BatchServerInventoryView;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.curator.CuratorUtils;
import org.apache.druid.curator.discovery.LatchableServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.compact.NewestSegmentFirstPolicy;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.KillSupervisorsCustomDuty;
import org.apache.druid.server.coordinator.loading.CuratorLoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidCoordinatorTest extends CuratorTestBase
{
  private static final String LOADPATH = "/druid/loadqueue/localhost:1234";
  private static final long COORDINATOR_START_DELAY = 1;
  private static final long COORDINATOR_PERIOD = 100;

  private DruidCoordinator coordinator;
  private SegmentsMetadataManager segmentsMetadataManager;
  private DataSourcesSnapshot dataSourcesSnapshot;

  private BatchServerInventoryView serverInventoryView;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private DruidServer druidServer;
  private LoadQueuePeon loadQueuePeon;
  private LoadQueueTaskMaster loadQueueTaskMaster;
  private MetadataRuleManager metadataRuleManager;
  private CountDownLatch leaderAnnouncerLatch;
  private CountDownLatch leaderUnannouncerLatch;
  private PathChildrenCache pathChildrenCache;
  private DruidCoordinatorConfig druidCoordinatorConfig;
  private ObjectMapper objectMapper;
  private DruidNode druidNode;
  private NewestSegmentFirstPolicy newestSegmentFirstPolicy;
  private final LatchableServiceEmitter serviceEmitter = new LatchableServiceEmitter();

  @Before
  public void setUp() throws Exception
  {
    druidServer = new DruidServer("from", "from", null, 5L, ServerType.HISTORICAL, "tier1", 0);
    serverInventoryView = EasyMock.createMock(BatchServerInventoryView.class);
    segmentsMetadataManager = EasyMock.createNiceMock(SegmentsMetadataManager.class);
    dataSourcesSnapshot = EasyMock.createNiceMock(DataSourcesSnapshot.class);
    metadataRuleManager = EasyMock.createNiceMock(MetadataRuleManager.class);
    loadQueueTaskMaster = EasyMock.createMock(LoadQueueTaskMaster.class);

    JacksonConfigManager configManager = EasyMock.createNiceMock(JacksonConfigManager.class);
    EasyMock.expect(
        configManager.watch(
            EasyMock.eq(CoordinatorDynamicConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference<>(CoordinatorDynamicConfig.builder().build())).anyTimes();
    EasyMock.expect(
        configManager.watch(
            EasyMock.eq(CoordinatorCompactionConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference<>(CoordinatorCompactionConfig.empty())).anyTimes();
    EasyMock.replay(configManager);
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    curator.create().creatingParentsIfNeeded().forPath(LOADPATH);
    objectMapper = new DefaultObjectMapper();
    newestSegmentFirstPolicy = new NewestSegmentFirstPolicy(objectMapper);
    druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withCoordinatorStartDelay(new Duration(COORDINATOR_START_DELAY))
        .withCoordinatorPeriod(new Duration(COORDINATOR_PERIOD))
        .build();
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
    druidNode = new DruidNode("hey", "what", false, 1234, null, true, false);
    scheduledExecutorFactory = ScheduledExecutors::fixed;
    leaderAnnouncerLatch = new CountDownLatch(1);
    leaderUnannouncerLatch = new CountDownLatch(1);
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(configManager),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        null,
        loadQueueTaskMaster,
        new SegmentLoadQueueManager(serverInventoryView, loadQueueTaskMaster),
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        new CoordinatorCustomDutyGroups(ImmutableSet.of()),
        new CostBalancerStrategyFactory(),
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null
    );
  }

  private MetadataManager createMetadataManager(JacksonConfigManager configManager)
  {
    return new MetadataManager(
        null,
        new CoordinatorConfigManager(configManager, null, null),
        segmentsMetadataManager,
        null,
        metadataRuleManager,
        null
    );
  }

  @After
  public void tearDown() throws Exception
  {
    loadQueuePeon.stop();
    pathChildrenCache.close();
    tearDownServerAndCurator();
  }

  @Test(timeout = 60_000L)
  public void testCoordinatorRun() throws Exception
  {
    String dataSource = "dataSource1";
    String tier = "hot";

    // Setup MetadataRuleManager
    Rule foreverLoadRule = new ForeverLoadRule(ImmutableMap.of(tier, 2), null);
    EasyMock.expect(metadataRuleManager.getRulesWithDefault(EasyMock.anyString()))
            .andReturn(ImmutableList.of(foreverLoadRule)).atLeastOnce();

    metadataRuleManager.stop();
    EasyMock.expectLastCall().once();

    EasyMock.replay(metadataRuleManager);

    // Setup SegmentsMetadataManager
    DruidDataSource[] dataSources = {
        new DruidDataSource(dataSource, Collections.emptyMap())
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
    dataSources[0].addSegment(dataSegment);

    setupSegmentsMetadataMock(dataSources[0]);
    ImmutableDruidDataSource immutableDruidDataSource = EasyMock.createNiceMock(ImmutableDruidDataSource.class);
    EasyMock.expect(immutableDruidDataSource.getSegments())
            .andReturn(ImmutableSet.of(dataSegment)).atLeastOnce();
    EasyMock.replay(immutableDruidDataSource);

    // Setup ServerInventoryView
    druidServer = new DruidServer("server1", "localhost", null, 5L, ServerType.HISTORICAL, tier, 0);
    setupPeons(Collections.singletonMap("server1", loadQueuePeon));
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(
        ImmutableList.of(druidServer)
    ).atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();
    EasyMock.replay(serverInventoryView, loadQueueTaskMaster);

    coordinator.start();

    Assert.assertNull(coordinator.getReplicationFactor(dataSegment.getId()));

    // Wait for this coordinator to become leader
    leaderAnnouncerLatch.await();

    // This coordinator should be leader by now
    Assert.assertTrue(coordinator.isLeader());
    Assert.assertEquals(druidNode.getHostAndPort(), coordinator.getCurrentLeader());
    pathChildrenCache.start();

    final CountDownLatch assignSegmentLatch = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(
        1,
        pathChildrenCache,
        ImmutableMap.of("2010-01-01T00:00:00.000Z_2010-01-02T00:00:00.000Z", dataSegment),
        druidServer
    );
    assignSegmentLatch.await();

    final CountDownLatch coordinatorRunLatch = new CountDownLatch(2);
    serviceEmitter.latch = coordinatorRunLatch;
    coordinatorRunLatch.await();

    Assert.assertEquals(ImmutableMap.of(dataSource, 100.0), coordinator.getDatasourceToLoadStatus());

    Object2IntMap<String> numsUnavailableUsedSegmentsPerDataSource =
        coordinator.getDatasourceToUnavailableSegmentCount();
    Assert.assertEquals(1, numsUnavailableUsedSegmentsPerDataSource.size());
    Assert.assertEquals(0, numsUnavailableUsedSegmentsPerDataSource.getInt(dataSource));

    Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier =
        coordinator.getTierToDatasourceToUnderReplicatedCount(false);
    Assert.assertNotNull(underReplicationCountsPerDataSourcePerTier);
    Assert.assertEquals(1, underReplicationCountsPerDataSourcePerTier.size());

    Object2LongMap<String> underRepliicationCountsPerDataSource = underReplicationCountsPerDataSourcePerTier.get(tier);
    Assert.assertNotNull(underRepliicationCountsPerDataSource);
    Assert.assertEquals(1, underRepliicationCountsPerDataSource.size());
    //noinspection deprecation
    Assert.assertNotNull(underRepliicationCountsPerDataSource.get(dataSource));
    // Simulated the adding of segment to druidServer during SegmentChangeRequestLoad event
    // The load rules asks for 2 replicas, therefore 1 replica should still be pending
    Assert.assertEquals(1L, underRepliicationCountsPerDataSource.getLong(dataSource));

    Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTierUsingClusterView =
        coordinator.getTierToDatasourceToUnderReplicatedCount(true);
    Assert.assertNotNull(underReplicationCountsPerDataSourcePerTier);
    Assert.assertEquals(1, underReplicationCountsPerDataSourcePerTier.size());

    Object2LongMap<String> underRepliicationCountsPerDataSourceUsingClusterView =
        underReplicationCountsPerDataSourcePerTierUsingClusterView.get(tier);
    Assert.assertNotNull(underRepliicationCountsPerDataSourceUsingClusterView);
    Assert.assertEquals(1, underRepliicationCountsPerDataSourceUsingClusterView.size());
    //noinspection deprecation
    Assert.assertNotNull(underRepliicationCountsPerDataSourceUsingClusterView.get(dataSource));
    // Simulated the adding of segment to druidServer during SegmentChangeRequestLoad event
    // The load rules asks for 2 replicas, but only 1 historical server in cluster. Since computing using cluster view
    // the segments are replicated as many times as they can be given state of cluster, therefore should not be
    // under-replicated.
    Assert.assertEquals(0L, underRepliicationCountsPerDataSourceUsingClusterView.getLong(dataSource));
    Assert.assertEquals(Integer.valueOf(2), coordinator.getReplicationFactor(dataSegment.getId()));

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
    final Rule hotTier = new IntervalLoadRule(Intervals.of("2018-01-01/P1M"), ImmutableMap.of(hotTierName, 1), null);
    final Rule coldTier = new ForeverLoadRule(ImmutableMap.of(coldTierName, 1), null);
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
        curator,
        loadPathCold,
        true,
        true,
        Execs.singleThreaded("coordinator_test_path_children_cache_cold-%d")
    );
    setupPeons(ImmutableMap.of("hot", loadQueuePeon, "cold", loadQueuePeonCold));

    loadQueuePeonCold.start();
    pathChildrenCache.start();
    pathChildrenCacheCold.start();

    DruidDataSource[] druidDataSources = {new DruidDataSource(dataSource, Collections.emptyMap())};
    dataSegments.values().forEach(druidDataSources[0]::addSegment);

    setupSegmentsMetadataMock(druidDataSources[0]);

    EasyMock.expect(metadataRuleManager.getRulesWithDefault(EasyMock.anyString()))
            .andReturn(ImmutableList.of(hotTier, coldTier)).atLeastOnce();

    EasyMock.expect(serverInventoryView.getInventory())
            .andReturn(ImmutableList.of(hotServer, coldServer))
            .atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();

    EasyMock.replay(metadataRuleManager, serverInventoryView, loadQueueTaskMaster);

    coordinator.start();
    leaderAnnouncerLatch.await(); // Wait for this coordinator to become leader

    final CountDownLatch assignSegmentLatchHot = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(2, pathChildrenCache, dataSegments, hotServer);
    final CountDownLatch assignSegmentLatchCold = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(1, pathChildrenCacheCold, dataSegments, coldServer);
    assignSegmentLatchHot.await();
    assignSegmentLatchCold.await();

    final CountDownLatch coordinatorRunLatch = new CountDownLatch(2);
    serviceEmitter.latch = coordinatorRunLatch;
    coordinatorRunLatch.await();

    Assert.assertEquals(ImmutableMap.of(dataSource, 100.0), coordinator.getDatasourceToLoadStatus());

    Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier =
        coordinator.getTierToDatasourceToUnderReplicatedCount(false);
    Assert.assertEquals(2, underReplicationCountsPerDataSourcePerTier.size());
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTier.get(hotTierName).getLong(dataSource));
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTier.get(coldTierName).getLong(dataSource));

    Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTierUsingClusterView =
        coordinator.getTierToDatasourceToUnderReplicatedCount(true);
    Assert.assertEquals(2, underReplicationCountsPerDataSourcePerTierUsingClusterView.size());
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTierUsingClusterView.get(hotTierName).getLong(dataSource));
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTierUsingClusterView.get(coldTierName).getLong(dataSource));

    dataSegments.values().forEach(dataSegment -> Assert.assertEquals(Integer.valueOf(1), coordinator.getReplicationFactor(dataSegment.getId())));

    coordinator.stop();
    leaderUnannouncerLatch.await();

    EasyMock.verify(serverInventoryView);
    EasyMock.verify(segmentsMetadataManager);
    EasyMock.verify(metadataRuleManager);
  }

  @Test(timeout = 60_000L)
  public void testComputeUnderReplicationCountsPerDataSourcePerTierForSegmentsWithBroadcastRule() throws Exception
  {
    final String dataSource = "dataSource";
    final String hotTierName = "hot";
    final String coldTierName = "cold";
    final String tierName1 = "tier1";
    final String tierName2 = "tier2";
    final String loadPathCold = "/druid/loadqueue/cold:1234";
    final String loadPathBroker1 = "/druid/loadqueue/broker1:1234";
    final String loadPathBroker2 = "/druid/loadqueue/broker2:1234";
    final String loadPathPeon = "/druid/loadqueue/peon:1234";
    final DruidServer hotServer = new DruidServer("hot", "hot", null, 5L, ServerType.HISTORICAL, hotTierName, 0);
    final DruidServer coldServer = new DruidServer("cold", "cold", null, 5L, ServerType.HISTORICAL, coldTierName, 0);
    final DruidServer brokerServer1 = new DruidServer("broker1", "broker1", null, 5L, ServerType.BROKER, tierName1, 0);
    final DruidServer brokerServer2 = new DruidServer("broker2", "broker2", null, 5L, ServerType.BROKER, tierName2, 0);
    final DruidServer peonServer = new DruidServer("peon", "peon", null, 5L, ServerType.INDEXER_EXECUTOR, tierName2, 0);

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

    final LoadQueuePeon loadQueuePeonBroker1 = new CuratorLoadQueuePeon(
        curator,
        loadPathBroker1,
        objectMapper,
        Execs.scheduledSingleThreaded("coordinator_test_load_queue_peon_broker1_scheduled-%d"),
        Execs.singleThreaded("coordinator_test_load_queue_peon_broker1-%d"),
        druidCoordinatorConfig
    );

    final LoadQueuePeon loadQueuePeonBroker2 = new CuratorLoadQueuePeon(
        curator,
        loadPathBroker2,
        objectMapper,
        Execs.scheduledSingleThreaded("coordinator_test_load_queue_peon_broker2_scheduled-%d"),
        Execs.singleThreaded("coordinator_test_load_queue_peon_broker2-%d"),
        druidCoordinatorConfig
    );

    final LoadQueuePeon loadQueuePeonPoenServer = new CuratorLoadQueuePeon(
        curator,
        loadPathPeon,
        objectMapper,
        Execs.scheduledSingleThreaded("coordinator_test_load_queue_peon_peon_scheduled-%d"),
        Execs.singleThreaded("coordinator_test_load_queue_peon_peon-%d"),
        druidCoordinatorConfig
    );
    final PathChildrenCache pathChildrenCacheCold = new PathChildrenCache(
        curator,
        loadPathCold,
        true,
        true,
        Execs.singleThreaded("coordinator_test_path_children_cache_cold-%d")
    );
    final PathChildrenCache pathChildrenCacheBroker1 = new PathChildrenCache(
        curator,
        loadPathBroker1,
        true,
        true,
        Execs.singleThreaded("coordinator_test_path_children_cache_broker1-%d")
    );
    final PathChildrenCache pathChildrenCacheBroker2 = new PathChildrenCache(
        curator,
        loadPathBroker2,
        true,
        true,
        Execs.singleThreaded("coordinator_test_path_children_cache_broker2-%d")
    );
    final PathChildrenCache pathChildrenCachePeon = new PathChildrenCache(
        curator,
        loadPathPeon,
        true,
        true,
        Execs.singleThreaded("coordinator_test_path_children_cache_peon-%d")
    );

    setupPeons(ImmutableMap.of(
        "hot", loadQueuePeon,
        "cold", loadQueuePeonCold,
        "broker1", loadQueuePeonBroker1,
        "broker2", loadQueuePeonBroker2,
        "peon", loadQueuePeonPoenServer
    ));

    loadQueuePeonCold.start();
    loadQueuePeonBroker1.start();
    loadQueuePeonBroker2.start();
    loadQueuePeonPoenServer.start();
    pathChildrenCache.start();
    pathChildrenCacheCold.start();
    pathChildrenCacheBroker1.start();
    pathChildrenCacheBroker2.start();
    pathChildrenCachePeon.start();

    DruidDataSource druidDataSource = new DruidDataSource(dataSource, Collections.emptyMap());
    dataSegments.values().forEach(druidDataSource::addSegment);

    setupSegmentsMetadataMock(druidDataSource);

    final Rule broadcastDistributionRule = new ForeverBroadcastDistributionRule();
    EasyMock.expect(metadataRuleManager.getRulesWithDefault(EasyMock.anyString()))
            .andReturn(ImmutableList.of(broadcastDistributionRule)).atLeastOnce();

    EasyMock.expect(serverInventoryView.getInventory())
            .andReturn(ImmutableList.of(hotServer, coldServer, brokerServer1, brokerServer2, peonServer))
            .atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();

    EasyMock.replay(metadataRuleManager, serverInventoryView, loadQueueTaskMaster);

    coordinator.start();
    leaderAnnouncerLatch.await(); // Wait for this coordinator to become leader

    final CountDownLatch assignSegmentLatchHot = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(3, pathChildrenCache, dataSegments, hotServer);
    final CountDownLatch assignSegmentLatchCold = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(3, pathChildrenCacheCold, dataSegments, coldServer);
    final CountDownLatch assignSegmentLatchBroker1 = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(3, pathChildrenCacheBroker1, dataSegments, brokerServer1);
    final CountDownLatch assignSegmentLatchBroker2 = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(3, pathChildrenCacheBroker2, dataSegments, brokerServer2);
    final CountDownLatch assignSegmentLatchPeon = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(3, pathChildrenCachePeon, dataSegments, peonServer);
    assignSegmentLatchHot.await();
    assignSegmentLatchCold.await();
    assignSegmentLatchBroker1.await();
    assignSegmentLatchBroker2.await();
    assignSegmentLatchPeon.await();

    final CountDownLatch coordinatorRunLatch = new CountDownLatch(2);
    serviceEmitter.latch = coordinatorRunLatch;
    coordinatorRunLatch.await();

    Assert.assertEquals(ImmutableMap.of(dataSource, 100.0), coordinator.getDatasourceToLoadStatus());

    // Under-replicated counts are updated only after the next coordinator run
    Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier =
        coordinator.getTierToDatasourceToUnderReplicatedCount(false);
    Assert.assertEquals(4, underReplicationCountsPerDataSourcePerTier.size());
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTier.get(hotTierName).getLong(dataSource));
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTier.get(coldTierName).getLong(dataSource));
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTier.get(tierName1).getLong(dataSource));
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTier.get(tierName2).getLong(dataSource));

    Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTierUsingClusterView =
        coordinator.getTierToDatasourceToUnderReplicatedCount(true);
    Assert.assertEquals(4, underReplicationCountsPerDataSourcePerTierUsingClusterView.size());
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTierUsingClusterView.get(hotTierName).getLong(dataSource));
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTierUsingClusterView.get(coldTierName).getLong(dataSource));
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTierUsingClusterView.get(tierName1).getLong(dataSource));
    Assert.assertEquals(0L, underReplicationCountsPerDataSourcePerTierUsingClusterView.get(tierName2).getLong(dataSource));

    coordinator.stop();
    leaderUnannouncerLatch.await();

    EasyMock.verify(serverInventoryView);
    EasyMock.verify(segmentsMetadataManager);
    EasyMock.verify(metadataRuleManager);
  }


  @Test
  public void testCompactSegmentsDutyWhenCustomDutyGroupEmpty()
  {
    CoordinatorCustomDutyGroups emptyCustomDutyGroups = new CoordinatorCustomDutyGroups(ImmutableSet.of());
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(null),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        null,
        loadQueueTaskMaster,
        null,
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        emptyCustomDutyGroups,
        new CostBalancerStrategyFactory(),
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null
    );
    // Since CompactSegments is not enabled in Custom Duty Group, then CompactSegments must be created in IndexingServiceDuties
    List<CoordinatorDuty> indexingDuties = coordinator.makeIndexingServiceDuties();
    Assert.assertTrue(indexingDuties.stream().anyMatch(coordinatorDuty -> coordinatorDuty instanceof CompactSegments));

    // CompactSegments should not exist in Custom Duty Group
    List<CompactSegments> compactSegmentsDutyFromCustomGroups = coordinator.getCompactSegmentsDutyFromCustomGroups();
    Assert.assertTrue(compactSegmentsDutyFromCustomGroups.isEmpty());

    // CompactSegments returned by this method should be created using the DruidCoordinatorConfig in the DruidCoordinator
    CompactSegments duty = coordinator.initializeCompactSegmentsDuty(newestSegmentFirstPolicy);
    Assert.assertNotNull(duty);
  }

  @Test
  public void testInitializeCompactSegmentsDutyWhenCustomDutyGroupDoesNotContainsCompactSegments()
  {
    CoordinatorCustomDutyGroup group = new CoordinatorCustomDutyGroup(
        "group1",
        Duration.standardSeconds(1),
        ImmutableList.of(new KillSupervisorsCustomDuty(new Duration("PT1S"), null, druidCoordinatorConfig))
    );
    CoordinatorCustomDutyGroups customDutyGroups = new CoordinatorCustomDutyGroups(ImmutableSet.of(group));
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(null),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        null,
        loadQueueTaskMaster,
        null,
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        customDutyGroups,
        new CostBalancerStrategyFactory(),
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null
    );
    // Since CompactSegments is not enabled in Custom Duty Group, then CompactSegments must be created in IndexingServiceDuties
    List<CoordinatorDuty> indexingDuties = coordinator.makeIndexingServiceDuties();
    Assert.assertTrue(indexingDuties.stream().anyMatch(coordinatorDuty -> coordinatorDuty instanceof CompactSegments));

    // CompactSegments should not exist in Custom Duty Group
    List<CompactSegments> compactSegmentsDutyFromCustomGroups = coordinator.getCompactSegmentsDutyFromCustomGroups();
    Assert.assertTrue(compactSegmentsDutyFromCustomGroups.isEmpty());

    // CompactSegments returned by this method should be created using the DruidCoordinatorConfig in the DruidCoordinator
    CompactSegments duty = coordinator.initializeCompactSegmentsDuty(newestSegmentFirstPolicy);
    Assert.assertNotNull(duty);
  }

  @Test
  public void testInitializeCompactSegmentsDutyWhenCustomDutyGroupContainsCompactSegments()
  {
    CoordinatorCustomDutyGroup compactSegmentCustomGroup = new CoordinatorCustomDutyGroup(
        "group1",
        Duration.standardSeconds(1),
        ImmutableList.of(new CompactSegments(null, null))
    );
    CoordinatorCustomDutyGroups customDutyGroups = new CoordinatorCustomDutyGroups(ImmutableSet.of(compactSegmentCustomGroup));
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(null),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        null,
        loadQueueTaskMaster,
        null,
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        customDutyGroups,
        new CostBalancerStrategyFactory(),
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null
    );
    // Since CompactSegments is enabled in Custom Duty Group, then CompactSegments must not be created in IndexingServiceDuties
    List<CoordinatorDuty> indexingDuties = coordinator.makeIndexingServiceDuties();
    Assert.assertTrue(indexingDuties.stream().noneMatch(coordinatorDuty -> coordinatorDuty instanceof CompactSegments));

    // CompactSegments should exist in Custom Duty Group
    List<CompactSegments> compactSegmentsDutyFromCustomGroups = coordinator.getCompactSegmentsDutyFromCustomGroups();
    Assert.assertFalse(compactSegmentsDutyFromCustomGroups.isEmpty());
    Assert.assertEquals(1, compactSegmentsDutyFromCustomGroups.size());
    Assert.assertNotNull(compactSegmentsDutyFromCustomGroups.get(0));

    // CompactSegments returned by this method should be from the Custom Duty Group
    CompactSegments duty = coordinator.initializeCompactSegmentsDuty(newestSegmentFirstPolicy);
    Assert.assertNotNull(duty);
  }

  @Test(timeout = 3000)
  public void testCoordinatorCustomDutyGroupsRunAsExpected() throws Exception
  {
    // Some nessesary setup to start the Coordinator
    setupPeons(Collections.emptyMap());
    JacksonConfigManager configManager = EasyMock.createNiceMock(JacksonConfigManager.class);
    EasyMock.expect(
        configManager.watch(
            EasyMock.eq(CoordinatorDynamicConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference<>(CoordinatorDynamicConfig.builder().build())).anyTimes();
    EasyMock.expect(
        configManager.watch(
            EasyMock.eq(CoordinatorCompactionConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference<>(CoordinatorCompactionConfig.empty())).anyTimes();
    EasyMock.replay(configManager);
    DruidDataSource dataSource = new DruidDataSource("dataSource1", Collections.emptyMap());
    DataSegment dataSegment = new DataSegment(
        "dataSource1",
        Intervals.of("2010-01-01/P1D"),
        "v1",
        null,
        null,
        null,
        null,
        0x9,
        0
    );
    dataSource.addSegment(dataSegment);
    DataSourcesSnapshot dataSourcesSnapshot =
        new DataSourcesSnapshot(ImmutableMap.of(dataSource.getName(), dataSource.toImmutableDruidDataSource()));
    EasyMock
        .expect(segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments())
        .andReturn(dataSourcesSnapshot)
        .anyTimes();
    EasyMock.expect(segmentsMetadataManager.isPollingDatabasePeriodically()).andReturn(true).anyTimes();
    EasyMock.expect(segmentsMetadataManager.iterateAllUsedSegments())
            .andReturn(Collections.singletonList(dataSegment)).anyTimes();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.replay(serverInventoryView, loadQueueTaskMaster, segmentsMetadataManager);

    // Create CoordinatorCustomDutyGroups
    // We will have two groups and each group has one duty
    CountDownLatch latch1 = new CountDownLatch(1);
    CoordinatorCustomDuty duty1 = params -> {
      latch1.countDown();
      return params;
    };
    CoordinatorCustomDutyGroup group1 = new CoordinatorCustomDutyGroup(
        "group1",
        Duration.standardSeconds(1),
        ImmutableList.of(duty1)
    );

    CountDownLatch latch2 = new CountDownLatch(1);
    CoordinatorCustomDuty duty2 = params -> {
      latch2.countDown();
      return params;
    };
    CoordinatorCustomDutyGroup group2 = new CoordinatorCustomDutyGroup(
        "group2",
        Duration.standardSeconds(1),
        ImmutableList.of(duty2)
    );
    CoordinatorCustomDutyGroups groups = new CoordinatorCustomDutyGroups(ImmutableSet.of(group1, group2));

    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(configManager),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        null,
        loadQueueTaskMaster,
        new SegmentLoadQueueManager(serverInventoryView, loadQueueTaskMaster),
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        groups,
        new CostBalancerStrategyFactory(),
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null
    );
    coordinator.start();

    // Wait until group 1 duty ran for latch1 to countdown
    latch1.await();
    // Wait until group 2 duty ran for latch2 to countdown
    latch2.await();
  }

  @Test(timeout = 60_000L)
  public void testCoordinatorRun_queryFromDeepStorage() throws Exception
  {
    String dataSource = "dataSource1";

    String coldTier = "coldTier";
    String hotTier = "hotTier";

    // Setup MetadataRuleManager
    Rule intervalLoadRule = new IntervalLoadRule(Intervals.of("2010-02-01/P1M"), ImmutableMap.of(hotTier, 1), null);
    Rule foreverLoadRule = new ForeverLoadRule(ImmutableMap.of(coldTier, 0), null);
    EasyMock.expect(metadataRuleManager.getRulesWithDefault(EasyMock.anyString()))
        .andReturn(ImmutableList.of(intervalLoadRule, foreverLoadRule)).atLeastOnce();

    metadataRuleManager.stop();
    EasyMock.expectLastCall().once();

    EasyMock.replay(metadataRuleManager);

    // Setup SegmentsMetadataManager
    DruidDataSource[] dataSources = {
        new DruidDataSource(dataSource, Collections.emptyMap())

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
    final DataSegment dataSegmentHot = new DataSegment(
        dataSource,
        Intervals.of("2010-02-01/P1D"),
        "v1",
        null,
        null,
        null,
        null,
        0x9,
        0
    );
    dataSources[0].addSegment(dataSegment).addSegment(dataSegmentHot);

    setupSegmentsMetadataMock(dataSources[0]);
    ImmutableDruidDataSource immutableDruidDataSource = EasyMock.createNiceMock(ImmutableDruidDataSource.class);
    EasyMock.expect(immutableDruidDataSource.getSegments())
        .andReturn(ImmutableSet.of(dataSegment, dataSegmentHot)).atLeastOnce();
    EasyMock.replay(immutableDruidDataSource);

    // Setup ServerInventoryView
    druidServer = new DruidServer("server1", "localhost", null, 5L, ServerType.HISTORICAL, hotTier, 0);
    DruidServer druidServer2 = new DruidServer("server2", "localhost", null, 5L, ServerType.HISTORICAL, coldTier, 0);
    setupPeons(ImmutableMap.of("server1", loadQueuePeon, "server2", loadQueuePeon));
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(
        ImmutableList.of(druidServer, druidServer2)
    ).atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();
    EasyMock.replay(serverInventoryView, loadQueueTaskMaster);

    coordinator.start();
    
    // Wait for this coordinator to become leader
    leaderAnnouncerLatch.await();

    // This coordinator should be leader by now
    Assert.assertTrue(coordinator.isLeader());
    Assert.assertEquals(druidNode.getHostAndPort(), coordinator.getCurrentLeader());
    pathChildrenCache.start();

    final CountDownLatch coordinatorRunLatch = new CountDownLatch(2);
    serviceEmitter.latch = coordinatorRunLatch;
    coordinatorRunLatch.await();

    Object2IntMap<String> numsUnavailableUsedSegmentsPerDataSource =
        coordinator.getDatasourceToUnavailableSegmentCount();
    Assert.assertEquals(1, numsUnavailableUsedSegmentsPerDataSource.size());
    // The cold tier segment should not be unavailable, the hot one should be unavailable
    Assert.assertEquals(1, numsUnavailableUsedSegmentsPerDataSource.getInt(dataSource));

    Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier =
        coordinator.getTierToDatasourceToUnderReplicatedCount(false);
    Assert.assertNotNull(underReplicationCountsPerDataSourcePerTier);
    Assert.assertEquals(2, underReplicationCountsPerDataSourcePerTier.size());

    Object2LongMap<String> underRepliicationCountsPerDataSourceHotTier = underReplicationCountsPerDataSourcePerTier.get(hotTier);
    Assert.assertNotNull(underRepliicationCountsPerDataSourceHotTier);
    Assert.assertEquals(1, underRepliicationCountsPerDataSourceHotTier.getLong(dataSource));

    Object2LongMap<String> underRepliicationCountsPerDataSourceColdTier = underReplicationCountsPerDataSourcePerTier.get(coldTier);
    Assert.assertNotNull(underRepliicationCountsPerDataSourceColdTier);
    Assert.assertEquals(0, underRepliicationCountsPerDataSourceColdTier.getLong(dataSource));

    Object2IntMap<String> numsDeepStorageOnlySegmentsPerDataSource =
            coordinator.getDatasourceToDeepStorageQueryOnlySegmentCount();

    Assert.assertEquals(1, numsDeepStorageOnlySegmentsPerDataSource.getInt(dataSource));

    coordinator.stop();
    leaderUnannouncerLatch.await();

    Assert.assertFalse(coordinator.isLeader());
    Assert.assertNull(coordinator.getCurrentLeader());

    EasyMock.verify(serverInventoryView);
    EasyMock.verify(metadataRuleManager);
  }

  private CountDownLatch createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(
      int latchCount,
      PathChildrenCache pathChildrenCache,
      Map<String, DataSegment> segments,
      DruidServer server
  )
  {
    final CountDownLatch countDownLatch = new CountDownLatch(latchCount);
    pathChildrenCache.getListenable().addListener(
        (CuratorFramework client, PathChildrenCacheEvent event) -> {
          if (CuratorUtils.isChildAdded(event)) {
            DataSegment segment = findSegmentRelatedToCuratorEvent(segments, event);
            if (segment != null && server.getSegment(segment.getId()) == null) {
              if (countDownLatch.getCount() > 0) {
                server.addDataSegment(segment);
                curator.delete().guaranteed().forPath(event.getData().getPath());
                countDownLatch.countDown();
              } else {
                Assert.fail("The segment path " + event.getData().getPath() + " is not expected");
              }
            }
          }
        }
    );
    return countDownLatch;
  }

  private void setupSegmentsMetadataMock(DruidDataSource dataSource)
  {
    EasyMock.expect(segmentsMetadataManager.isPollingDatabasePeriodically()).andReturn(true).anyTimes();
    EasyMock
        .expect(segmentsMetadataManager.iterateAllUsedSegments())
        .andReturn(dataSource.getSegments())
        .anyTimes();
    EasyMock
        .expect(segmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments())
        .andReturn(Collections.singleton(dataSource.toImmutableDruidDataSource()))
        .anyTimes();
    DataSourcesSnapshot dataSourcesSnapshot =
        new DataSourcesSnapshot(ImmutableMap.of(dataSource.getName(), dataSource.toImmutableDruidDataSource()));
    EasyMock
        .expect(segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments())
        .andReturn(dataSourcesSnapshot)
        .anyTimes();
    EasyMock
        .expect(segmentsMetadataManager.retrieveAllDataSourceNames())
        .andReturn(Collections.singleton(dataSource.getName()))
        .anyTimes();
    EasyMock.replay(segmentsMetadataManager);

    EasyMock
        .expect(this.dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot())
        .andReturn(dataSource.getSegments())
        .anyTimes();
    EasyMock
        .expect(this.dataSourcesSnapshot.getDataSourcesWithAllUsedSegments())
        .andReturn(Collections.singleton(dataSource.toImmutableDruidDataSource()))
        .anyTimes();
    EasyMock.replay(this.dataSourcesSnapshot);
  }

  @Nullable
  private static DataSegment findSegmentRelatedToCuratorEvent(
      Map<String, DataSegment> dataSegments,
      PathChildrenCacheEvent event
  )
  {
    return dataSegments
        .entrySet()
        .stream()
        .filter(x -> event.getData().getPath().contains(x.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }

  private void setupPeons(Map<String, LoadQueuePeon> peonMap)
  {
    loadQueueTaskMaster.resetPeonsForNewServers(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    loadQueueTaskMaster.onLeaderStart();
    EasyMock.expectLastCall().anyTimes();
    loadQueueTaskMaster.onLeaderStop();
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(loadQueueTaskMaster.getAllPeons()).andReturn(peonMap).anyTimes();

    EasyMock.expect(loadQueueTaskMaster.getPeonForServer(EasyMock.anyObject())).andAnswer(
        () -> peonMap.get(((ImmutableDruidServer) EasyMock.getCurrentArgument(0)).getName())
    ).anyTimes();
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
