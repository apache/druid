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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ImmutableDruidServerTests;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class UnloadUnusedSegmentsTest
{
  private DruidCoordinator coordinator;
  private ImmutableDruidServer historicalServer;
  private ImmutableDruidServer historicalServerTier2;
  private ImmutableDruidServer brokerServer;
  private ImmutableDruidServer indexerServer;
  private TestLoadQueuePeon historicalPeon;
  private TestLoadQueuePeon historicalTier2Peon;
  private TestLoadQueuePeon brokerPeon;
  private TestLoadQueuePeon indexerPeon;
  private DataSegment segment1;
  private DataSegment segment2;
  private List<DataSegment> segments;
  private List<DataSegment> segmentsForRealtime;
  private List<ImmutableDruidDataSource> dataSources;
  private List<ImmutableDruidDataSource> dataSourcesForRealtime;
  private final String broadcastDatasource = "broadcastDatasource";
  private MetadataRuleManager databaseRuleManager;
  private SegmentLoadQueueManager loadQueueManager;

  @Before
  public void setUp()
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    historicalServer = EasyMock.createMock(ImmutableDruidServer.class);
    historicalServerTier2 = EasyMock.createMock(ImmutableDruidServer.class);
    brokerServer = EasyMock.createMock(ImmutableDruidServer.class);
    indexerServer = EasyMock.createMock(ImmutableDruidServer.class);
    databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);
    loadQueueManager = new SegmentLoadQueueManager(null, null, null);

    DateTime start1 = DateTimes.of("2012-01-01");
    DateTime start2 = DateTimes.of("2012-02-01");
    DateTime version = DateTimes.of("2012-05-01");
    segment1 = new DataSegment(
        "datasource1",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    segment2 = new DataSegment(
        "datasource2",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    final DataSegment realtimeOnlySegment = new DataSegment(
        "datasource2",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    final DataSegment broadcastSegment = new DataSegment(
        broadcastDatasource,
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        7L
    );

    segments = new ArrayList<>();
    segments.add(segment1);
    segments.add(segment2);
    segments.add(broadcastSegment);

    segmentsForRealtime = new ArrayList<>();
    segmentsForRealtime.add(realtimeOnlySegment);
    segmentsForRealtime.add(broadcastSegment);

    historicalPeon = new TestLoadQueuePeon();
    historicalTier2Peon = new TestLoadQueuePeon();
    brokerPeon = new TestLoadQueuePeon();
    indexerPeon = new TestLoadQueuePeon();

    final ImmutableDruidDataSource dataSource1 = new ImmutableDruidDataSource(
        "datasource1",
        Collections.emptyMap(),
        Collections.singleton(segment1)
    );
    final ImmutableDruidDataSource dataSource2 = new ImmutableDruidDataSource(
        "datasource2",
        Collections.emptyMap(),
        Collections.singleton(segment2)
    );

    final ImmutableDruidDataSource broadcastDatasource = new ImmutableDruidDataSource(
        "broadcastDatasource",
        Collections.emptyMap(),
        Collections.singleton(broadcastSegment)
    );

    dataSources = ImmutableList.of(dataSource1, dataSource2, broadcastDatasource);

    // This simulates a task that is ingesting to an existing non-broadcast datasource, with unpublished segments,
    // while also having a broadcast segment loaded.
    final ImmutableDruidDataSource dataSource2ForRealtime = new ImmutableDruidDataSource(
        "datasource2",
        Collections.emptyMap(),
        Collections.singleton(realtimeOnlySegment)
    );
    dataSourcesForRealtime = ImmutableList.of(dataSource2ForRealtime, broadcastDatasource);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(historicalServer);
    EasyMock.verify(historicalServerTier2);
    EasyMock.verify(brokerServer);
    EasyMock.verify(indexerServer);
    EasyMock.verify(databaseRuleManager);
  }

  @Test
  public void test_unloadUnusedSegmentsFromAllServers()
  {
    mockDruidServer(
        historicalServer,
        ServerType.HISTORICAL,
        "historical",
        DruidServer.DEFAULT_TIER,
        30L,
        100L,
        segments,
        dataSources
    );
    mockDruidServer(
        historicalServerTier2,
        ServerType.HISTORICAL,
        "historicalTier2",
        "tier2",
        30L,
        100L,
        segments,
        dataSources
    );
    mockDruidServer(
        brokerServer,
        ServerType.BROKER,
        "broker",
        DruidServer.DEFAULT_TIER,
        30L,
        100L,
        segments,
        dataSources
    );
    mockDruidServer(
        indexerServer,
        ServerType.INDEXER_EXECUTOR,
        "indexer",
        DruidServer.DEFAULT_TIER,
        30L,
        100L,
        segmentsForRealtime,
        dataSourcesForRealtime
    );

    // Mock stuff that the coordinator needs
    mockCoordinator(coordinator);

    mockRuleManager(databaseRuleManager);

    // We keep datasource2 segments only, drop datasource1 and broadcastDatasource from all servers
    // realtimeSegment is intentionally missing from the set, to match how a realtime tasks's unpublished segments
    // will not appear in the coordinator's view of used segments.
    Set<DataSegment> usedSegments = ImmutableSet.of(segment2);

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(
            DruidCluster
                .builder()
                .addTier(
                    DruidServer.DEFAULT_TIER,
                    new ServerHolder(historicalServer, historicalPeon, false)
                )
                .addTier(
                    "tier2",
                    new ServerHolder(historicalServerTier2, historicalTier2Peon, false)
                )
                .addBrokers(new ServerHolder(brokerServer, brokerPeon, false))
                .addRealtimes(new ServerHolder(indexerServer, indexerPeon, false))
                .build()
        )
        .withUsedSegmentsInTest(usedSegments)
        .withBroadcastDatasources(Collections.singleton(broadcastDatasource))
        .withDatabaseRuleManager(databaseRuleManager)
        .build();

    params = new UnloadUnusedSegments(loadQueueManager).run(params);
    CoordinatorRunStats stats = params.getCoordinatorStats();

    // We drop segment1 and broadcast1 from all servers, realtimeSegment is not dropped by the indexer
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.UNNEEDED, DruidServer.DEFAULT_TIER, segment1.getDataSource()));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.UNNEEDED, "tier2", segment1.getDataSource()));

    Assert.assertEquals(3L, stats.getSegmentStat(Stats.Segments.UNNEEDED, DruidServer.DEFAULT_TIER, broadcastDatasource));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.UNNEEDED, "tier2", broadcastDatasource));
  }

  private static void mockDruidServer(
      ImmutableDruidServer druidServer,
      ServerType serverType,
      String name,
      String tier,
      long currentSize,
      long maxSize,
      List<DataSegment> segments,
      List<ImmutableDruidDataSource> dataSources
  )
  {
    EasyMock.expect(druidServer.getName()).andReturn(name).anyTimes();
    EasyMock.expect(druidServer.getTier()).andReturn(tier).anyTimes();
    EasyMock.expect(druidServer.getCurrSize()).andReturn(currentSize).anyTimes();
    EasyMock.expect(druidServer.getMaxSize()).andReturn(maxSize).anyTimes();
    ImmutableDruidServerTests.expectSegments(druidServer, segments);
    EasyMock.expect(druidServer.getHost()).andReturn(name).anyTimes();
    EasyMock.expect(druidServer.getType()).andReturn(serverType).anyTimes();
    EasyMock.expect(druidServer.getDataSources()).andReturn(dataSources).anyTimes();
    if (!segments.isEmpty()) {
      segments.forEach(
          s -> EasyMock.expect(druidServer.getSegment(s.getId())).andReturn(s).anyTimes()
      );
    }
    EasyMock.expect(druidServer.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer);
  }

  private static void mockCoordinator(DruidCoordinator coordinator)
  {
    EasyMock.replay(coordinator);
  }

  private static void mockRuleManager(MetadataRuleManager metadataRuleManager)
  {
    EasyMock.expect(metadataRuleManager.getRulesWithDefault("datasource1")).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(
                    DruidServer.DEFAULT_TIER, 1,
                    "tier2", 1
                ),
                null
            )
        )).anyTimes();

    EasyMock.expect(metadataRuleManager.getRulesWithDefault("datasource2")).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(
                    DruidServer.DEFAULT_TIER, 1,
                    "tier2", 1
                ),
                null
            )
        )).anyTimes();

    EasyMock.expect(metadataRuleManager.getRulesWithDefault("broadcastDatasource")).andReturn(
        Collections.singletonList(
            new ForeverBroadcastDistributionRule()
        )).anyTimes();

    EasyMock.replay(metadataRuleManager);
  }
}
