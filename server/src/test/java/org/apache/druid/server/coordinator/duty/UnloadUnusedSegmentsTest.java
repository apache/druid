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
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidClusterBuilder;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
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
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class UnloadUnusedSegmentsTest
{
  private DruidCoordinator coordinator;
  private ImmutableDruidServer historicalServer;
  private ImmutableDruidServer historicalServerTier2;
  private ImmutableDruidServer brokerServer;
  private ImmutableDruidServer indexerServer;
  private LoadQueuePeonTester historicalPeon;
  private LoadQueuePeonTester historicalTier2Peon;
  private LoadQueuePeonTester brokerPeon;
  private LoadQueuePeonTester indexerPeon;
  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment broadcastSegment;
  private DataSegment realtimeOnlySegment;
  private List<DataSegment> segments;
  private List<DataSegment> segmentsForRealtime;
  private ImmutableDruidDataSource dataSource1;
  private ImmutableDruidDataSource dataSource2;
  private ImmutableDruidDataSource dataSource2ForRealtime;
  private ImmutableDruidDataSource broadcastDatasource;
  private List<ImmutableDruidDataSource> dataSources;
  private List<ImmutableDruidDataSource> dataSourcesForRealtime;
  private Set<String> broadcastDatasourceNames;
  private MetadataRuleManager databaseRuleManager;

  @Before
  public void setUp()
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    historicalServer = EasyMock.createMock(ImmutableDruidServer.class);
    historicalServerTier2 = EasyMock.createMock(ImmutableDruidServer.class);
    brokerServer = EasyMock.createMock(ImmutableDruidServer.class);
    indexerServer = EasyMock.createMock(ImmutableDruidServer.class);
    segment1 = EasyMock.createMock(DataSegment.class);
    segment2 = EasyMock.createMock(DataSegment.class);
    databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);

    DateTime start1 = DateTimes.of("2012-01-01");
    DateTime start2 = DateTimes.of("2012-02-01");
    DateTime version = DateTimes.of("2012-05-01");
    segment1 = new DataSegment(
        "datasource1",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    segment2 = new DataSegment(
        "datasource2",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    realtimeOnlySegment = new DataSegment(
        "datasource2",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    broadcastSegment = new DataSegment(
        "broadcastDatasource",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
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

    historicalPeon = new LoadQueuePeonTester();
    historicalTier2Peon = new LoadQueuePeonTester();
    brokerPeon = new LoadQueuePeonTester();
    indexerPeon = new LoadQueuePeonTester();

    dataSource1 = new ImmutableDruidDataSource(
        "datasource1",
        Collections.emptyMap(),
        Collections.singleton(segment1)
    );
    dataSource2 = new ImmutableDruidDataSource(
        "datasource2",
        Collections.emptyMap(),
        Collections.singleton(segment2)
    );

    broadcastDatasourceNames = Collections.singleton("broadcastDatasource");
    broadcastDatasource = new ImmutableDruidDataSource(
        "broadcastDatasource",
        Collections.emptyMap(),
        Collections.singleton(broadcastSegment)
    );

    dataSources = ImmutableList.of(dataSource1, dataSource2, broadcastDatasource);

    // This simulates a task that is ingesting to an existing non-broadcast datasource, with unpublished segments,
    // while also having a broadcast segment loaded.
    dataSource2ForRealtime = new ImmutableDruidDataSource(
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

    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(
            DruidClusterBuilder
                .newBuilder()
                .addTier(
                    DruidServer.DEFAULT_TIER,
                    new ServerHolder(historicalServer, historicalPeon, false)
                )
                .addTier(
                    "tier2",
                    new ServerHolder(historicalServerTier2, historicalTier2Peon, false)
                )
                .withBrokers(
                    new ServerHolder(brokerServer, brokerPeon, false)
                )
                .withRealtimes(
                    new ServerHolder(indexerServer, indexerPeon, false)
                )
                .build()
        )
        .withLoadManagementPeons(
            ImmutableMap.of(
                "historical", historicalPeon,
                "historicalTier2", historicalTier2Peon,
                "broker", brokerPeon,
                "indexer", indexerPeon
            )
        )
        .withUsedSegmentsInTest(usedSegments)
        .withBroadcastDatasources(broadcastDatasourceNames)
        .withDatabaseRuleManager(databaseRuleManager)
        .build();

    params = new UnloadUnusedSegments().run(params);
    CoordinatorStats stats = params.getCoordinatorStats();

    // We drop segment1 and broadcast1 from all servers, realtimeSegment is not dropped by the indexer
    Assert.assertEquals(5, stats.getTieredStat("unneededCount", DruidServer.DEFAULT_TIER));
    Assert.assertEquals(2, stats.getTieredStat("unneededCount", "tier2"));
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
    coordinator.moveSegment(
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
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
                )
            )
        )).anyTimes();

    EasyMock.expect(metadataRuleManager.getRulesWithDefault("datasource2")).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(
                    DruidServer.DEFAULT_TIER, 1,
                    "tier2", 1
                )
            )
        )).anyTimes();

    EasyMock.expect(metadataRuleManager.getRulesWithDefault("broadcastDatasource")).andReturn(
        Collections.singletonList(
            new ForeverBroadcastDistributionRule()
        )).anyTimes();

    EasyMock.replay(metadataRuleManager);
  }
}
