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
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;

import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.SingleServerInventoryView;
import io.druid.collections.CountingMap;
import io.druid.common.config.JacksonConfigManager;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import io.druid.curator.discovery.NoopServiceAnnouncer;
import io.druid.curator.inventory.InventoryManagerConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
  private LoadQueueTaskMaster taskMaster;
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
  private static final String LOADPATH = "/druid/loadqueue/localhost:1234";
  private static final long COORDINATOR_START_DELAY = 1;
  private static final long COORDINATOR_PERIOD = 100;

  @Before
  public void setUp() throws Exception
  {
    taskMaster = EasyMock.createMock(LoadQueueTaskMaster.class);
    druidServer = EasyMock.createMock(DruidServer.class);
    serverInventoryView = EasyMock.createMock(SingleServerInventoryView.class);
    databaseSegmentManager = EasyMock.createNiceMock(MetadataSegmentManager.class);
    metadataRuleManager = EasyMock.createNiceMock(MetadataRuleManager.class);
    configManager = EasyMock.createNiceMock(JacksonConfigManager.class);
    EasyMock.expect(
        configManager.watch(
            EasyMock.anyString(),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference(new CoordinatorDynamicConfig.Builder().build())).anyTimes();
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
        false
    );
    pathChildrenCache = new PathChildrenCache(curator, LOADPATH, true, true, Execs.singleThreaded("coordinator_test_path_children_cache-%d"));
    loadQueuePeon = new LoadQueuePeon(
      curator,
      LOADPATH,
      objectMapper,
      Execs.scheduledSingleThreaded("coordinator_test_load_queue_peon_scheduled-%d"),
      Execs.singleThreaded("coordinator_test_load_queue_peon-%d"),
      druidCoordinatorConfig
    );
    druidNode = new DruidNode("hey", "what", 1234);
    loadManagementPeons = new MapMaker().makeMap();
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
        new NoopServiceEmitter(),
        scheduledExecutorFactory,
        null,
        taskMaster,
        new NoopServiceAnnouncer(){
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
        null
    );
  }

  @After
  public void tearDown() throws Exception
  {
    pathChildrenCache.close();
    tearDownServerAndCurator();
  }

  @Test
  public void testMoveSegment() throws Exception
  {
    loadQueuePeon = EasyMock.createNiceMock(LoadQueuePeon.class);
    EasyMock.expect(loadQueuePeon.getLoadQueueSize()).andReturn(new Long(1));
    EasyMock.replay(loadQueuePeon);
    segment = EasyMock.createNiceMock(DataSegment.class);
    EasyMock.replay(segment);
    scheduledExecutorFactory = EasyMock.createNiceMock(ScheduledExecutorFactory.class);
    EasyMock.replay(scheduledExecutorFactory);
    EasyMock.replay(metadataRuleManager);
    EasyMock.expect(druidServer.toImmutableDruidServer()).andReturn(
        new ImmutableDruidServer(
            new DruidServerMetadata("from", null, 5L, null, null, 0),
            1L,
            null,
            ImmutableMap.of("dummySegment", segment)
        )
    ).atLeastOnce();
    EasyMock.replay(druidServer);

    druidServer2 = EasyMock.createMock(DruidServer.class);
    EasyMock.expect(druidServer2.toImmutableDruidServer()).andReturn(
        new ImmutableDruidServer(
            new DruidServerMetadata("to", null, 5L, null, null, 0),
            1L,
            null,
            ImmutableMap.of("dummySegment2", segment)
        )
    ).atLeastOnce();
    EasyMock.replay(druidServer2);

    loadManagementPeons.put("from", loadQueuePeon);
    loadManagementPeons.put("to", loadQueuePeon);

    EasyMock.expect(serverInventoryView.getInventoryManagerConfig()).andReturn(
        new InventoryManagerConfig()
        {
          @Override
          public String getContainerPath()
          {
            return "";
          }

          @Override
          public String getInventoryPath()
          {
            return "";
          }
        }
    );
    EasyMock.replay(serverInventoryView);

    coordinator.moveSegment(
        druidServer.toImmutableDruidServer(),
        druidServer2.toImmutableDruidServer(),
        "dummySegment", null
    );
    EasyMock.verify(druidServer);
    EasyMock.verify(druidServer2);
    EasyMock.verify(loadQueuePeon);
    EasyMock.verify(serverInventoryView);
    EasyMock.verify(metadataRuleManager);
  }

  @Test(timeout = 60_000L)
  public void testCoordinatorRun() throws Exception{
    String dataSource = "dataSource1";
    String tier= "hot";

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
    final DataSegment dataSegment = new DataSegment(dataSource, new Interval("2010-01-01/P1D"), "v1", null, null, null, null, 0x9, 0);
    druidDataSources[0].addSegment("0", dataSegment);

    EasyMock.expect(databaseSegmentManager.isStarted()).andReturn(true).anyTimes();
    EasyMock.expect(databaseSegmentManager.getInventory()).andReturn(
        ImmutableList.of(druidDataSources[0])
    ).atLeastOnce();
    EasyMock.replay(databaseSegmentManager);
    ImmutableDruidDataSource immutableDruidDataSource = EasyMock.createNiceMock(ImmutableDruidDataSource.class);
    EasyMock.expect(immutableDruidDataSource.getSegments())
            .andReturn(ImmutableSet.of(dataSegment)).atLeastOnce();
    EasyMock.replay(immutableDruidDataSource);

    // Setup ServerInventoryView
    druidServer = new DruidServer("server1", "localhost", 5L, "historical", tier, 0);
    loadManagementPeons.put("server1", loadQueuePeon);
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(
        ImmutableList.of(druidServer)
    ).atLeastOnce();
    serverInventoryView.start();
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();

    serverInventoryView.stop();
    EasyMock.expectLastCall().once();

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
        ) throws Exception
        {
          if(pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
            //Coordinator should try to assign segment to druidServer historical
            //Simulate historical loading segment
            druidServer.addDataSegment(dataSegment.getIdentifier(), dataSegment);
            assignSegmentLatch.countDown();
          }
        }
      }
    );
    pathChildrenCache.start();

    assignSegmentLatch.await();

    Assert.assertEquals(ImmutableMap.of(dataSource, 100.0), coordinator.getLoadStatus());
    curator.delete().guaranteed().forPath(ZKPaths.makePath(LOADPATH, dataSegment.getIdentifier()));
    // Wait for coordinator thread to run so that replication status is updated
    while (coordinator.getSegmentAvailability().snapshot().get(dataSource) != 0) {
      Thread.sleep(50);
    }
    Map segmentAvailability = coordinator.getSegmentAvailability().snapshot();
    Assert.assertEquals(1, segmentAvailability.size());
    Assert.assertEquals(0L, segmentAvailability.get(dataSource));

    while (coordinator.getLoadPendingDatasources().get(dataSource).get() > 0) {
      Thread.sleep(50);
    }

    // wait historical data to be updated
    long startMillis = System.currentTimeMillis();
    long coordinatorRunPeriodMillis = druidCoordinatorConfig.getCoordinatorPeriod().getMillis();
    while (System.currentTimeMillis() - startMillis < coordinatorRunPeriodMillis) {
      Thread.sleep(100);
    }

    Map<String, CountingMap<String>> replicationStatus = coordinator.getReplicationStatus();
    Assert.assertNotNull(replicationStatus);
    Assert.assertEquals(1, replicationStatus.entrySet().size());

    CountingMap<String> dataSourceMap = replicationStatus.get(tier);
    Assert.assertNotNull(dataSourceMap);
    Assert.assertEquals(1, dataSourceMap.size());
    Assert.assertNotNull(dataSourceMap.get(dataSource));
    // Simulated the adding of segment to druidServer during SegmentChangeRequestLoad event
    // The load rules asks for 2 replicas, therefore 1 replica should still be pending
    while(dataSourceMap.get(dataSource).get() != 1L) {
      Thread.sleep(50);
    }

    coordinator.stop();
    leaderUnannouncerLatch.await();

    Assert.assertFalse(coordinator.isLeader());
    Assert.assertNull(coordinator.getCurrentLeader());

    EasyMock.verify(serverInventoryView);
    EasyMock.verify(metadataRuleManager);
  }

  @Test
  public void testOrderedAvailableDataSegments()
  {
    DruidDataSource dataSource = new DruidDataSource("test", new HashMap());
    DataSegment[] segments = new DataSegment[]{
        getSegment("test", new Interval("2016-01-10T03:00:00Z/2016-01-10T04:00:00Z")),
        getSegment("test", new Interval("2016-01-11T01:00:00Z/2016-01-11T02:00:00Z")),
        getSegment("test", new Interval("2016-01-09T10:00:00Z/2016-01-09T11:00:00Z")),
        getSegment("test", new Interval("2016-01-09T10:00:00Z/2016-01-09T12:00:00Z"))
    };
    for (DataSegment segment : segments) {
      dataSource.addSegment(segment.getIdentifier(), segment);
    }

    EasyMock.expect(databaseSegmentManager.getInventory()).andReturn(
        ImmutableList.of(dataSource)
    ).atLeastOnce();
    EasyMock.replay(databaseSegmentManager);
    Set<DataSegment> availableSegments = coordinator.getOrderedAvailableDataSegments();
    DataSegment[] expected = new DataSegment[]{
        getSegment("test", new Interval("2016-01-11T01:00:00Z/2016-01-11T02:00:00Z")),
        getSegment("test", new Interval("2016-01-10T03:00:00Z/2016-01-10T04:00:00Z")),
        getSegment("test", new Interval("2016-01-09T10:00:00Z/2016-01-09T12:00:00Z")),
        getSegment("test", new Interval("2016-01-09T10:00:00Z/2016-01-09T11:00:00Z"))
    };
    Assert.assertEquals(expected.length, availableSegments.size());
    Assert.assertEquals(expected, availableSegments.toArray());
    EasyMock.verify(databaseSegmentManager);
  }


  private DataSegment getSegment(String dataSource, Interval interval)
  {
    // Not using EasyMock as it hampers the performance of multithreads.
    DataSegment segment = new DataSegment(
        dataSource, interval, "dummy_version", Maps.<String, Object>newConcurrentMap(),
        Lists.<String>newArrayList(), Lists.<String>newArrayList(), null, 0, 0L
    );
    return segment;
  }
}
