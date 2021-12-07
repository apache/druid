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
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.SingleServerInventoryView;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.curator.CuratorUtils;
import org.apache.druid.curator.ZkEnablementConfig;
import org.apache.druid.curator.discovery.NoopServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@SuppressWarnings("deprecation")
public class DruidCoordinatorMultithreadTest extends CuratorTestBase
{
  private static final Logger log = new Logger(DruidCoordinatorMultithreadTest.class);
  private static final String LOADPATH = "/druid/loadqueue/localhost:%d";
  private static final long COORDINATOR_START_DELAY = 1;

  // For this test the period for each task should be significantly longer than its run time.
  // Also, make sure the primary period is significantly less than the coordinator period.  We want the primary loader
  // to run twice, and the historical duties to run only once.  This is due to the way this test simulates
  // historical loading.
  private static final long COORDINATOR_PERIOD = 60_000L;
  private static final long PRIMARY_PERIOD = 10_000L;

  // These are parameters you can vary
  private static final int DATA_SOURCE_COUNT = 10;
  private static final int SEGMENT_COUNT = 200; // per datasource
  private static final int HISTORICAL_COUNT = 20; // number of historical servers to start

  private DruidCoordinator coordinator;
  private SegmentsMetadataManager segmentsMetadataManager;
  private DataSourcesSnapshot dataSourcesSnapshot;

  private SingleServerInventoryView serverInventoryView;
  private ConcurrentHashMap<String, LoadQueuePeon> loadManagementPeons;
  private MetadataRuleManager metadataRuleManager;
  private CountDownLatch leaderAnnouncerLatch;
  private CountDownLatch leaderUnannouncerLatch;
  private List<PathChildrenCache> pathChildrenCaches;
  private DruidNode druidNode;
  private final LatchableServiceEmitter serviceEmitter = new LatchableServiceEmitter();

  @Before
  public void setUp() throws Exception
  {
    serverInventoryView = EasyMock.createMock(SingleServerInventoryView.class);
    segmentsMetadataManager = EasyMock.createNiceMock(SegmentsMetadataManager.class);
    dataSourcesSnapshot = EasyMock.createNiceMock(DataSourcesSnapshot.class);
    metadataRuleManager = EasyMock.createNiceMock(MetadataRuleManager.class);
    JacksonConfigManager configManager = EasyMock.createNiceMock(JacksonConfigManager.class);
    EasyMock.expect(
        configManager.watch(
            EasyMock.eq(CoordinatorDynamicConfig.CONFIG_KEY),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject()
        )
    ).andReturn(new AtomicReference<>(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(0).build())).anyTimes();
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
    for (int i = 0; i < HISTORICAL_COUNT; i++) {
      curator.create().creatingParentsIfNeeded().forPath(String.format(Locale.ENGLISH, LOADPATH, i));
    }
    ObjectMapper objectMapper = new DefaultObjectMapper();
    DruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig(
        new Duration(COORDINATOR_START_DELAY),
        new Duration(COORDINATOR_PERIOD),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        10,
        new Duration("PT0s"),
        new Duration(PRIMARY_PERIOD),
        true,
        3
    );
    loadManagementPeons = new ConcurrentHashMap<>();
    pathChildrenCaches = new ArrayList<>();
    for (int i = 0; i < HISTORICAL_COUNT; i++) {
      LoadQueuePeon loadQueuePeon = new CuratorLoadQueuePeon(
          curator,
          String.format(Locale.ENGLISH, LOADPATH, i),
          objectMapper,
          Execs.scheduledSingleThreaded("coordinator_test_load_queue_peon_scheduled-%d"),
          Execs.singleThreaded("coordinator_test_load_queue_peon-%d"),
          druidCoordinatorConfig
      );
      loadManagementPeons.put("server_" + i, loadQueuePeon);
      loadQueuePeon.start();
      pathChildrenCaches.add(new PathChildrenCache(
          curator,
          String.format(Locale.ENGLISH, LOADPATH, i),
          true,
          true,
          Execs.singleThreaded("coordinator_test_path_children_cache_" + i + "-%d")
      ));
    }
    druidNode = new DruidNode("hey", "what", false, 1234, null, true, false);
    ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors::fixed;
    leaderAnnouncerLatch = new CountDownLatch(1);
    leaderUnannouncerLatch = new CountDownLatch(1);
    EmittingLogger.registerEmitter(serviceEmitter);
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
        segmentsMetadataManager,
        serverInventoryView,
        metadataRuleManager,
        () -> curator,
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
        Collections.emptySet(),
        new CoordinatorCustomDutyGroups(Collections.emptySet()),
        new CostBalancerStrategyFactory(),
        EasyMock.createNiceMock(LookupCoordinatorManager.class),
        new TestDruidLeaderSelector(),
        null,
        new ZkEnablementConfig(true)
    );
  }

  @After
  public void tearDown() throws Exception
  {
    loadManagementPeons.values().forEach(LoadQueuePeon::stop);
    pathChildrenCaches.forEach(pathChildrenCache -> {
      try {
        pathChildrenCache.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    tearDownServerAndCurator();
  }


  @Test(timeout = 60_000L)
  public void testCoordinatorRunMulti() throws Exception
  {
    // The primary loader loads all segments in its first run.  We then need it to run again to refresh the
    // coordinator's SegmentReplicantLookup to reflect the work it did in its first run.
    // Because of the way this test is written, we don't want the historical duties to run more than once.
    // Both tasks emit the segment/assigned/count metric, which we use as a signal to count the runs, so
    // to cover 2 primary runs and one historical run we want a value of 3.  This also means we want the run period
    // of the primary loader to be less than that of the historical duties.
    serviceEmitter.primaryLatch = new CountDownLatch(3);
    serviceEmitter.historicalLatch = new CountDownLatch(1);

    String dataSourceRoot = "dataSource";
    DruidDataSource[] dataSources = new DruidDataSource[DATA_SOURCE_COUNT];
    String tier = "hot";

    // Setup MetadataRuleManager
    Map<String, List<Rule>> ruleMap = new HashMap<>();
    List<Rule> rules = new ArrayList<>();
    for (int i = 0; i < DATA_SOURCE_COUNT; i++) {
      dataSources[i] = new DruidDataSource(dataSourceRoot + i, Collections.emptyMap());
      Rule foreverLoadRule = new ForeverLoadRule(ImmutableMap.of(tier, 1));
      ruleMap.put(dataSourceRoot + i, ImmutableList.of(foreverLoadRule));
      rules.add(foreverLoadRule);
    }
    EasyMock.expect(metadataRuleManager.getRulesWithDefault(EasyMock.anyString()))
            .andReturn(rules).atLeastOnce();
    EasyMock.expect(metadataRuleManager.getAllRules())
            .andReturn(ImmutableMap.copyOf(ruleMap)).atLeastOnce();

    metadataRuleManager.stop();
    EasyMock.expectLastCall().once();

    EasyMock.replay(metadataRuleManager);

    // Setup SegmentsMetadataManager
    ConcurrentMap<String, DataSegment> segmentMap = new ConcurrentHashMap<>();
    LocalDate date = LocalDate.of(2010, 1, 1);
    for (int i = 0; i < DATA_SOURCE_COUNT; i++) {
      for (int j = 0; j < SEGMENT_COUNT; j++) {
        date = date.plusDays(1L);
        String intervalString = DateTimeFormatter.ISO_LOCAL_DATE.format(date) + "/P1D";
        Interval interval = Intervals.of(intervalString);
        final DataSegment dataSegment = new DataSegment(
            dataSourceRoot + i,
            interval,
            "v1",
            null,
            null,
            null,
            null,
            0x9,
            0
        );
        dataSources[i].addSegment(dataSegment);
        segmentMap.put(
            dataSources[i].getName() + "_" + interval.toString().replace('/', '_'),
            dataSegment
        );
      }
    }

    setupSegmentsMetadataMock(dataSources);

    // Setup ServerInventoryView
    List<DruidServer> servers = new ArrayList<>();
    for (int i = 0; i < HISTORICAL_COUNT; i++) {
      servers.add(
          new DruidServer("server_" + i, "host_" + i, null, 5L, ServerType.HISTORICAL, tier, 0));
    }
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(
        servers
    ).atLeastOnce();
    EasyMock.expect(serverInventoryView.isStarted()).andReturn(true).anyTimes();
    Capture<String> serverCapture = Capture.newInstance();
    Capture<DataSegment> segmentCapture = Capture.newInstance();
    EasyMock.expect(serverInventoryView.isSegmentLoadedByServer(
                EasyMock.capture(serverCapture),
                EasyMock.capture(segmentCapture)
            ))
            .andAnswer(() -> null != findServer(servers, serverCapture.getValue()).getSegment(segmentCapture.getValue()
                                                                                                            .getId()))
            .anyTimes();
    EasyMock.replay(serverInventoryView);

    coordinator.start();
    // Wait for this coordinator to become leader
    leaderAnnouncerLatch.await();

    // This coordinator should be leader by now
    Assert.assertTrue(coordinator.isLeader());
    Assert.assertEquals(druidNode.getHostAndPort(), coordinator.getCurrentLeader());
    pathChildrenCaches.forEach(pathChildrenCache -> {
      try {
        pathChildrenCache.start();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    final CountDownLatch assignSegmentLatch = createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(
        segmentMap.size(),
        pathChildrenCaches,
        segmentMap,
        servers
    );
    assignSegmentLatch.await();

    serviceEmitter.primaryLatch.await();
    serviceEmitter.historicalLatch.await();

    Map<String, Double> loadStatus =
        Arrays.stream(dataSources).collect(Collectors.toMap(DruidDataSource::getName, d -> 100.0));
    Assert.assertEquals(loadStatus, coordinator.getLoadStatus());

    Object2IntMap<String> numsUnavailableUsedSegmentsPerDataSource =
        coordinator.computeNumsUnavailableUsedSegmentsPerDataSource();
    Assert.assertEquals(DATA_SOURCE_COUNT, numsUnavailableUsedSegmentsPerDataSource.size());
    for (int i = 0; i < DATA_SOURCE_COUNT; i++) {
      Assert.assertEquals(
          0,
          numsUnavailableUsedSegmentsPerDataSource.getInt(dataSources[i].getName())
      );
    }


    Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier =
        coordinator.computeUnderReplicationCountsPerDataSourcePerTier();
    Assert.assertNotNull(underReplicationCountsPerDataSourcePerTier);
    Assert.assertEquals(1, underReplicationCountsPerDataSourcePerTier.size());

    Object2LongMap<String> underRepliicationCountsPerDataSource =
        underReplicationCountsPerDataSourcePerTier.get(tier);
    Assert.assertNotNull(underRepliicationCountsPerDataSource);
    Assert.assertEquals(DATA_SOURCE_COUNT, underRepliicationCountsPerDataSource.size());
    for (int i = 0; i < DATA_SOURCE_COUNT; i++) {
      //noinspection deprecation
      Assert.assertNotNull(underRepliicationCountsPerDataSource.get(dataSources[i].getName()));
      Assert.assertEquals(0L, underRepliicationCountsPerDataSource.getLong(dataSources[i].getName()));
    }

    coordinator.stop();
    leaderUnannouncerLatch.await();

    Assert.assertFalse(coordinator.isLeader());
    Assert.assertNull(coordinator.getCurrentLeader());

    EasyMock.verify(serverInventoryView);
    EasyMock.verify(metadataRuleManager);
  }

  private DruidServer findServer(List<DruidServer> servers, String serverKey)
  {
    return servers.stream()
                  .filter(s -> s.getName().equals(serverKey))
                  .findFirst()
                  .orElseThrow(() -> new RuntimeException("can't find server with name: " + serverKey));
  }

  private CountDownLatch createCountDownLatchAndSetPathChildrenCacheListenerWithLatch(
      int latchCount,
      List<PathChildrenCache> pathChildrenCaches,
      Map<String, DataSegment> segments,
      List<DruidServer> servers
  )
  {
    final CountDownLatch countDownLatch = new CountDownLatch(latchCount);
    final ConcurrentMap<DataSegment, Integer> segmentsAdded = new ConcurrentHashMap<>();
    final Pattern pathPattern = Pattern.compile("/druid/loadqueue/localhost:(\\d+)/.*");
    pathChildrenCaches.forEach(c -> c.getListenable().addListener(
        (CuratorFramework client, PathChildrenCacheEvent event) -> {
          log.info("event: " + event);
          if (CuratorUtils.isChildAdded(event)) {
            DataSegment segment = findSegmentRelatedToCuratorEvent(segments, event);
            String path = event.getData().getPath();
            Matcher m = pathPattern.matcher(path);
            if (!m.find()) {
              throw new RuntimeException("failed to find server number in " + path);
            }
            int serverNumber = Integer.parseInt(m.group(1));
            if (segment != null && servers.get(serverNumber).getSegment(segment.getId()) == null) {
              if (countDownLatch.getCount() > 0) {
                servers.get(serverNumber).addDataSegment(segment);
                curator.delete().guaranteed().forPath(event.getData().getPath());
                // only count down for the first add; subsequent adds are due to segment balancing
                if (null == segmentsAdded.get(segment)) {
                  segmentsAdded.put(segment, 1);
                  countDownLatch.countDown();
                  log.info(
                      "segment %s; latch %d; segmentsAdded %s",
                      segment,
                      countDownLatch.getCount(),
                      segmentsAdded.size()
                  );
                }
              } else {
                // Probably the historical duties ran a second time.
                Assert.fail("The segment path " + event.getData().getPath() + " is not expected");
              }
            }
          }
        }
    ));
    return countDownLatch;
  }

  private void setupSegmentsMetadataMock(DruidDataSource[] dataSources)
  {
    Map<String, ImmutableDruidDataSource> snapshotMap =
        Arrays.stream(dataSources)
              .collect(
                  Collectors.toMap(
                      DruidDataSource::getName,
                      DruidDataSource::toImmutableDruidDataSource
                  ));
    DataSourcesSnapshot dataSourcesSnapshot = new DataSourcesSnapshot(snapshotMap);

    Collection<DataSegment> segments = new ArrayList<>();
    for (DruidDataSource dataSource : dataSources) {
      segments.addAll(dataSource.getSegments());
    }

    EasyMock.expect(
                segmentsMetadataManager.isPollingDatabasePeriodically())
            .andReturn(true).anyTimes();
    EasyMock
        .expect(segmentsMetadataManager.iterateAllUsedSegments())
        .andReturn(segments)
        .anyTimes();
    EasyMock
        .expect(segmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments())
        .andReturn(snapshotMap.values())
        .anyTimes();
    EasyMock
        .expect(segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments())
        .andReturn(dataSourcesSnapshot)
        .anyTimes();
    EasyMock
        .expect(segmentsMetadataManager.retrieveAllDataSourceNames())
        .andReturn(snapshotMap.keySet())
        .anyTimes();
    EasyMock.replay(segmentsMetadataManager);

    EasyMock
        .expect(this.dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot())
        .andReturn(segments)
        .anyTimes();
    EasyMock
        .expect(this.dataSourcesSnapshot.getDataSourcesWithAllUsedSegments())
        .andReturn(snapshotMap.values())
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
    private CountDownLatch primaryLatch;
    private CountDownLatch historicalLatch;

    private LatchableServiceEmitter()
    {
      super("", "", null);
    }

    @Override
    public void emit(Event event)
    {
      if (primaryLatch != null && "segment/assigned/count".equals(event.toMap().get("metric"))) {
        int segmentCount = ((Number) event.toMap().get("value")).intValue();
        log.info(
            "segment/assigned/count emitted; segment count = %d, latch count=%d",
            segmentCount,
            primaryLatch.getCount()
        );
        primaryLatch.countDown();
      }
      if (historicalLatch != null && "segment/overShadowed/count".equals(event.toMap().get("metric"))) {
        log.info("segment/overShadowed/count emitted");
        historicalLatch.countDown();
      }
    }
  }
}
