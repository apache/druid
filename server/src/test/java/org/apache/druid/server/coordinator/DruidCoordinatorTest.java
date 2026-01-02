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
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.curator.discovery.LatchableServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.segment.cache.NoopSegmentMetadataCache;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.PersistedCompactionStateManager;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.config.CoordinatorKillConfigs;
import org.apache.druid.server.coordinator.config.CoordinatorPeriodConfig;
import org.apache.druid.server.coordinator.config.CoordinatorRunConfig;
import org.apache.druid.server.coordinator.config.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.duty.DutyGroupStatus;
import org.apache.druid.server.coordinator.duty.KillSupervisorsCustomDuty;
import org.apache.druid.server.coordinator.loading.LoadPeonCallback;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.server.http.CoordinatorDynamicConfigSyncer;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidCoordinatorTest
{
  private static final long COORDINATOR_START_DELAY = 1;
  private static final long COORDINATOR_PERIOD = 100;
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  private DruidCoordinator coordinator;
  private SegmentsMetadataManager segmentsMetadataManager;
  private DataSourcesSnapshot dataSourcesSnapshot;

  private ServerInventoryView serverInventoryView;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private LoadQueueTaskMaster loadQueueTaskMaster;
  private MetadataRuleManager metadataRuleManager;
  private CountDownLatch leaderAnnouncerLatch;
  private CountDownLatch leaderUnannouncerLatch;
  private DruidCoordinatorConfig druidCoordinatorConfig;
  private DruidNode druidNode;
  private OverlordClient overlordClient;
  private CompactionStatusTracker statusTracker;
  private LatchableServiceEmitter serviceEmitter;
  private PersistedCompactionStateManager persistedCompactionStateManager;

  @Before
  public void setUp() throws Exception
  {
    serverInventoryView = EasyMock.createMock(ServerInventoryView.class);
    segmentsMetadataManager = EasyMock.createNiceMock(SegmentsMetadataManager.class);
    dataSourcesSnapshot = EasyMock.createNiceMock(DataSourcesSnapshot.class);
    metadataRuleManager = EasyMock.createNiceMock(MetadataRuleManager.class);
    loadQueueTaskMaster = EasyMock.createMock(LoadQueueTaskMaster.class);
    overlordClient = EasyMock.createMock(OverlordClient.class);
    persistedCompactionStateManager = EasyMock.createMock(PersistedCompactionStateManager.class);

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
            EasyMock.eq(DruidCompactionConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference<>(DruidCompactionConfig.empty())).anyTimes();
    EasyMock.replay(configManager);
    statusTracker = new CompactionStatusTracker();
    druidCoordinatorConfig = new DruidCoordinatorConfig(
        new CoordinatorRunConfig(new Duration(COORDINATOR_START_DELAY), new Duration(COORDINATOR_PERIOD)),
        new CoordinatorPeriodConfig(null, null),
        CoordinatorKillConfigs.DEFAULT,
        new CostBalancerStrategyFactory(),
        null
    );
    druidNode = new DruidNode("hey", "what", false, 1234, null, true, false);
    scheduledExecutorFactory = ScheduledExecutors::fixed;
    leaderAnnouncerLatch = new CountDownLatch(1);
    leaderUnannouncerLatch = new CountDownLatch(1);
    serviceEmitter = new LatchableServiceEmitter();
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(configManager),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        overlordClient,
        loadQueueTaskMaster,
        new SegmentLoadQueueManager(serverInventoryView, loadQueueTaskMaster),
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        new CoordinatorCustomDutyGroups(ImmutableSet.of()),
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null,
        CentralizedDatasourceSchemaConfig.create(),
        new CompactionStatusTracker(),
        EasyMock.niceMock(CoordinatorDynamicConfigSyncer.class),
        EasyMock.niceMock(CloneStatusManager.class)
    );
  }

  private MetadataManager createMetadataManager(JacksonConfigManager configManager)
  {
    return new MetadataManager(
        null,
        new CoordinatorConfigManager(configManager, null, null, null),
        segmentsMetadataManager,
        null,
        metadataRuleManager,
        null,
        null,
        NoopSegmentMetadataCache.instance(),
        persistedCompactionStateManager
    );
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
    final DruidServer druidServer = new DruidServer("server1", "localhost", null, 5L, ServerType.HISTORICAL, tier, 0);
    final LoadQueuePeon loadQueuePeon = createImmediateLoadPeonFor(druidServer);
    setupPeons(Collections.singletonMap("server1", loadQueuePeon));
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(
        ImmutableList.of(druidServer)
    ).atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();
    EasyMock.replay(serverInventoryView, loadQueueTaskMaster);

    coordinator.start();

    Assert.assertNull(coordinator.getReplicationFactor(dataSegment.getId()));
    Assert.assertNull(coordinator.getBroadcastSegments());

    // Wait for this coordinator to become leader
    leaderAnnouncerLatch.await();

    // This coordinator should be leader by now
    Assert.assertTrue(coordinator.isLeader());
    Assert.assertEquals(druidNode.getHostAndPort(), coordinator.getCurrentLeader());

    serviceEmitter.coordinatorRunLatch.await();

    Assert.assertEquals(ImmutableMap.of(dataSource, 100.0), coordinator.getDatasourceToLoadStatus());

    Object2IntMap<String> numsUnavailableUsedSegmentsPerDataSource =
        coordinator.getDatasourceToUnavailableSegmentCount();
    Assert.assertEquals(1, numsUnavailableUsedSegmentsPerDataSource.size());
    Assert.assertEquals(0, numsUnavailableUsedSegmentsPerDataSource.getInt(dataSource));
    Assert.assertEquals(0, coordinator.getBroadcastSegments().size());

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
    final DruidServer hotServer = new DruidServer("hot", "hot", null, 5L, ServerType.HISTORICAL, hotTierName, 0);
    final DruidServer coldServer = new DruidServer("cold", "cold", null, 5L, ServerType.HISTORICAL, coldTierName, 0);

    final Set<DataSegment> dataSegments = Set.of(
        new DataSegment(dataSource, Intervals.of("2018-01-02/P1D"), "v1", null, null, null, null, 0x9, 0),
        new DataSegment(dataSource, Intervals.of("2018-01-03/P1D"), "v1", null, null, null, null, 0x9, 0),
        new DataSegment(dataSource, Intervals.of("2017-01-01/P1D"), "v1", null, null, null, null, 0x9, 0)
    );

    final LoadQueuePeon loadQueuePeonHot = createImmediateLoadPeonFor(hotServer);
    final LoadQueuePeon loadQueuePeonCold = createImmediateLoadPeonFor(coldServer);
    setupPeons(ImmutableMap.of("hot", loadQueuePeonHot, "cold", loadQueuePeonCold));

    loadQueuePeonHot.start();
    loadQueuePeonCold.start();

    DruidDataSource[] druidDataSources = {new DruidDataSource(dataSource, Collections.emptyMap())};
    dataSegments.forEach(druidDataSources[0]::addSegment);

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

    serviceEmitter.coordinatorRunLatch.await();

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

    dataSegments.forEach(dataSegment -> Assert.assertEquals(Integer.valueOf(1), coordinator.getReplicationFactor(dataSegment.getId())));

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
    final DruidServer hotServer = new DruidServer("hot", "hot", null, 5L, ServerType.HISTORICAL, hotTierName, 0);
    final DruidServer coldServer = new DruidServer("cold", "cold", null, 5L, ServerType.HISTORICAL, coldTierName, 0);
    final DruidServer brokerServer1 = new DruidServer("broker1", "broker1", null, 5L, ServerType.BROKER, tierName1, 0);
    final DruidServer brokerServer2 = new DruidServer("broker2", "broker2", null, 5L, ServerType.BROKER, tierName2, 0);
    final DruidServer peonServer = new DruidServer("peon", "peon", null, 5L, ServerType.INDEXER_EXECUTOR, tierName2, 0);

    final Set<DataSegment> dataSegments = Set.of(
        new DataSegment(dataSource, Intervals.of("2018-01-02/P1D"), "v1", null, null, null, null, 0x9, 0),
        new DataSegment(dataSource, Intervals.of("2018-01-03/P1D"), "v1", null, null, null, null, 0x9, 0),
        new DataSegment(dataSource, Intervals.of("2017-01-01/P1D"), "v1", null, null, null, null, 0x9, 0)
    );

    final LoadQueuePeon loadQueuePeonHot = createImmediateLoadPeonFor(hotServer);
    final LoadQueuePeon loadQueuePeonCold = createImmediateLoadPeonFor(coldServer);
    final LoadQueuePeon loadQueuePeonBroker1 = createImmediateLoadPeonFor(brokerServer1);
    final LoadQueuePeon loadQueuePeonBroker2 = createImmediateLoadPeonFor(brokerServer2);
    final LoadQueuePeon loadQueuePeonPoenServer = createImmediateLoadPeonFor(peonServer);
    setupPeons(ImmutableMap.of(
        "hot", loadQueuePeonHot,
        "cold", loadQueuePeonCold,
        "broker1", loadQueuePeonBroker1,
        "broker2", loadQueuePeonBroker2,
        "peon", loadQueuePeonPoenServer
    ));

    loadQueuePeonHot.start();
    loadQueuePeonCold.start();
    loadQueuePeonBroker1.start();
    loadQueuePeonBroker2.start();
    loadQueuePeonPoenServer.start();

    DruidDataSource druidDataSource = new DruidDataSource(dataSource, Collections.emptyMap());
    dataSegments.forEach(druidDataSource::addSegment);

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

    serviceEmitter.coordinatorRunLatch.await();

    Assert.assertEquals(ImmutableMap.of(dataSource, 100.0), coordinator.getDatasourceToLoadStatus());
    Assert.assertEquals(dataSegments, coordinator.getBroadcastSegments());

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
    EasyMock.expect(segmentsMetadataManager.isPollingDatabasePeriodically())
            .andReturn(true).anyTimes();
    EasyMock.replay(segmentsMetadataManager);

    CoordinatorCustomDutyGroups emptyCustomDutyGroups = new CoordinatorCustomDutyGroups(ImmutableSet.of());
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(null),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        overlordClient,
        loadQueueTaskMaster,
        null,
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        emptyCustomDutyGroups,
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null,
        CentralizedDatasourceSchemaConfig.create(),
        new CompactionStatusTracker(),
        EasyMock.niceMock(CoordinatorDynamicConfigSyncer.class),
        EasyMock.niceMock(CloneStatusManager.class)
    );
    coordinator.start();

    // Since CompactSegments is not enabled in Custom Duty Group, then CompactSegments must be created in IndexingServiceDuties
    final List<DutyGroupStatus> duties = coordinator.getStatusOfDuties();
    Assert.assertEquals(3, duties.size());

    Assert.assertEquals("HistoricalManagementDuties", duties.get(0).getName());
    Assert.assertEquals("IndexingServiceDuties", duties.get(1).getName());
    Assert.assertEquals("MetadataStoreManagementDuties", duties.get(2).getName());

    final String compactDutyName = CompactSegments.class.getName();
    Assert.assertTrue(duties.get(1).getDutyNames().contains(compactDutyName));

    // CompactSegments should not exist in other duty groups
    Assert.assertFalse(duties.get(0).getDutyNames().contains(compactDutyName));
    Assert.assertFalse(duties.get(2).getDutyNames().contains(compactDutyName));

    coordinator.stop();
  }

  @Test
  public void testInitializeCompactSegmentsDutyWhenCustomDutyGroupDoesNotContainsCompactSegments()
  {
    EasyMock.expect(segmentsMetadataManager.isPollingDatabasePeriodically())
            .andReturn(true).anyTimes();
    EasyMock.replay(segmentsMetadataManager);
    CoordinatorCustomDutyGroup group = new CoordinatorCustomDutyGroup(
        "group1",
        Duration.standardSeconds(1),
        ImmutableList.of(new KillSupervisorsCustomDuty(new Duration("PT1S"), null))
    );
    CoordinatorCustomDutyGroups customDutyGroups = new CoordinatorCustomDutyGroups(ImmutableSet.of(group));
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(null),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        overlordClient,
        loadQueueTaskMaster,
        null,
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        customDutyGroups,
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null,
        CentralizedDatasourceSchemaConfig.create(),
        new CompactionStatusTracker(),
        EasyMock.niceMock(CoordinatorDynamicConfigSyncer.class),
        EasyMock.niceMock(CloneStatusManager.class)
    );
    coordinator.start();
    // Since CompactSegments is not enabled in Custom Duty Group, then CompactSegments must be created in IndexingServiceDuties
    final List<DutyGroupStatus> duties = coordinator.getStatusOfDuties();
    Assert.assertEquals(4, duties.size());

    Assert.assertEquals("HistoricalManagementDuties", duties.get(0).getName());
    Assert.assertEquals("IndexingServiceDuties", duties.get(1).getName());
    Assert.assertEquals("MetadataStoreManagementDuties", duties.get(2).getName());
    Assert.assertEquals("group1", duties.get(3).getName());

    final String compactDutyName = CompactSegments.class.getName();
    Assert.assertTrue(duties.get(1).getDutyNames().contains(compactDutyName));

    // CompactSegments should not exist in other duty groups
    Assert.assertFalse(duties.get(0).getDutyNames().contains(compactDutyName));
    Assert.assertFalse(duties.get(2).getDutyNames().contains(compactDutyName));

    coordinator.stop();
  }

  @Test
  public void testInitializeCompactSegmentsDutyWhenCustomDutyGroupContainsCompactSegments()
  {
    EasyMock.expect(segmentsMetadataManager.isPollingDatabasePeriodically())
            .andReturn(true).anyTimes();
    EasyMock.replay(segmentsMetadataManager);
    CoordinatorCustomDutyGroup compactSegmentCustomGroup = new CoordinatorCustomDutyGroup(
        "group1",
        Duration.standardSeconds(1),
        ImmutableList.of(new CompactSegments(statusTracker, null))
    );
    CoordinatorCustomDutyGroups customDutyGroups = new CoordinatorCustomDutyGroups(ImmutableSet.of(compactSegmentCustomGroup));
    coordinator = new DruidCoordinator(
        druidCoordinatorConfig,
        createMetadataManager(null),
        serverInventoryView,
        serviceEmitter,
        scheduledExecutorFactory,
        overlordClient,
        loadQueueTaskMaster,
        null,
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        customDutyGroups,
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null,
        CentralizedDatasourceSchemaConfig.create(),
        new CompactionStatusTracker(),
        EasyMock.niceMock(CoordinatorDynamicConfigSyncer.class),
        EasyMock.niceMock(CloneStatusManager.class)
    );
    coordinator.start();

    // Since CompactSegments is enabled in Custom Duty Group, then CompactSegments must not be created in IndexingServiceDuties
    final List<DutyGroupStatus> duties = coordinator.getStatusOfDuties();
    Assert.assertEquals(4, duties.size());

    Assert.assertEquals("HistoricalManagementDuties", duties.get(0).getName());
    Assert.assertEquals("IndexingServiceDuties", duties.get(1).getName());
    Assert.assertEquals("MetadataStoreManagementDuties", duties.get(2).getName());
    Assert.assertEquals("group1", duties.get(3).getName());

    // CompactSegments should exist in Custom Duty Group
    final String compactDutyName = CompactSegments.class.getName();
    Assert.assertTrue(duties.get(3).getDutyNames().contains(compactDutyName));

    // CompactSegments should not exist in other duty groups
    Assert.assertFalse(duties.get(0).getDutyNames().contains(compactDutyName));
    Assert.assertFalse(duties.get(1).getDutyNames().contains(compactDutyName));
    Assert.assertFalse(duties.get(2).getDutyNames().contains(compactDutyName));

    coordinator.stop();
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
            EasyMock.eq(DruidCompactionConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference<>(DruidCompactionConfig.empty())).anyTimes();
    EasyMock.replay(configManager);
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
    DataSourcesSnapshot dataSourcesSnapshot = DataSourcesSnapshot.fromUsedSegments(
        Collections.singleton(dataSegment)
    );
    EasyMock.expect(segmentsMetadataManager.getRecentDataSourcesSnapshot())
            .andReturn(dataSourcesSnapshot).anyTimes();
    EasyMock.expect(segmentsMetadataManager.isPollingDatabasePeriodically()).andReturn(true).anyTimes();
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
        overlordClient,
        loadQueueTaskMaster,
        new SegmentLoadQueueManager(serverInventoryView, loadQueueTaskMaster),
        new LatchableServiceAnnouncer(leaderAnnouncerLatch, leaderUnannouncerLatch),
        druidNode,
        groups,
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null,
        CentralizedDatasourceSchemaConfig.create(),
        new CompactionStatusTracker(),
        EasyMock.niceMock(CoordinatorDynamicConfigSyncer.class),
        EasyMock.niceMock(CloneStatusManager.class)
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
    final DruidServer druidServer1 = new DruidServer("server1", "localhost", null, 5L, ServerType.HISTORICAL, hotTier, 0);
    final DruidServer druidServer2 = new DruidServer("server2", "localhost", null, 5L, ServerType.HISTORICAL, coldTier, 0);

    // For hot server, use a load queue peon that does not perform immediate load
    setupPeons(ImmutableMap.of(
        "server1", new TestLoadQueuePeon(),
        "server2", createImmediateLoadPeonFor(druidServer2))
    );
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(
        ImmutableList.of(druidServer1, druidServer2)
    ).atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();
    EasyMock.replay(serverInventoryView, loadQueueTaskMaster);

    coordinator.start();
    
    // Wait for this coordinator to become leader
    leaderAnnouncerLatch.await();

    // This coordinator should be leader by now
    Assert.assertTrue(coordinator.isLeader());
    Assert.assertEquals(druidNode.getHostAndPort(), coordinator.getCurrentLeader());

    serviceEmitter.coordinatorRunLatch.await();

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

  @Test
  public void testSimulateRunWithEmptyDatasourceCompactionConfigs()
  {
    DataSourcesSnapshot dataSourcesSnapshot = DataSourcesSnapshot.fromUsedSegments(Collections.emptyList());
    EasyMock
        .expect(segmentsMetadataManager.getRecentDataSourcesSnapshot())
        .andReturn(dataSourcesSnapshot)
        .anyTimes();
    EasyMock.replay(segmentsMetadataManager);
    CompactionSimulateResult result = coordinator.simulateRunWithConfigUpdate(
        new ClusterCompactionConfig(0.2, null, null, null, null, null)
    );
    Assert.assertEquals(Collections.emptyMap(), result.getCompactionStates());
  }

  private void setupSegmentsMetadataMock(DruidDataSource dataSource)
  {
    EasyMock.expect(segmentsMetadataManager.isPollingDatabasePeriodically()).andReturn(true).anyTimes();
    DataSourcesSnapshot dataSourcesSnapshot =
        DataSourcesSnapshot.fromUsedSegments(dataSource.getSegments());
    EasyMock
        .expect(segmentsMetadataManager.getRecentDataSourcesSnapshot())
        .andReturn(dataSourcesSnapshot)
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
  
  private LoadQueuePeon createImmediateLoadPeonFor(DruidServer server)
  {
    return new TestLoadQueuePeon() {
      @Override
      public void loadSegment(DataSegment segment, SegmentAction action, @Nullable LoadPeonCallback callback)
      {
        server.addDataSegment(segment);
        super.loadSegment(segment, action, callback);
      }
    };
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
    private final CountDownLatch coordinatorRunLatch = new CountDownLatch(2);

    private LatchableServiceEmitter()
    {
      super("", "", null);
    }

    @Override
    public void emit(Event event)
    {
      if (event instanceof ServiceMetricEvent) {
        final ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;

        // Count down when the historical management duties group has finished
        String dutyGroupName = (String) metricEvent.getUserDims().get("dutyGroup");
        if (Stats.CoordinatorRun.GROUP_RUN_TIME.getMetricName().equals(metricEvent.getMetric())
            && "HistoricalManagementDuties".equals(dutyGroupName)) {
          coordinatorRunLatch.countDown();
        }
      }
    }
  }
}
