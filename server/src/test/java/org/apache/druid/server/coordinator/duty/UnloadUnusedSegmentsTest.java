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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorBaseTest;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.server.coordinator.simulate.TestMetadataRuleManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

public class UnloadUnusedSegmentsTest extends CoordinatorBaseTest
{
  private MetadataRuleManager databaseRuleManager;
  private SegmentLoadQueueManager loadQueueManager;

  @Before
  public void setUp()
  {
    databaseRuleManager = new TestMetadataRuleManager();
    loadQueueManager = new SegmentLoadQueueManager(null, null);
  }

  @Test
  public void test_unloadUnusedSegmentsFromAllServers()
  {
    setupRuleManager();

    final DataSegment segmentWiki = CreateDataSegments.ofDatasource(DS.WIKI).eachOfSizeInMb(500).get(0);
    final DataSegment segmentKoala = CreateDataSegments.ofDatasource(DS.KOALA).eachOfSizeInMb(500).get(0);
    final DataSegment broadcastSegment = CreateDataSegments.ofDatasource(DS.BROADCAST).eachOfSizeInMb(500).get(0);
    final DataSegment realtimeSegment = CreateDataSegments.ofDatasource(DS.WIKI).eachOfSizeInMb(500).get(0);

    // We keep datasource2 segments only, drop datasource1 and broadcastDatasource from all servers
    // realtimeSegment is intentionally missing from the set, to match how a realtime tasks's unpublished segments
    // will not appear in the coordinator's view of used segments.
    Set<DataSegment> usedSegments = ImmutableSet.of(segmentKoala);

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(
            DruidCluster
                .builder()
                .add(
                    new ServerHolder(
                        new DruidServer("histT1", "histT1", null, 100L, ServerType.HISTORICAL, Tier.T1, 1)
                            .addDataSegment(segmentWiki).addDataSegment(segmentKoala).addDataSegment(broadcastSegment)
                            .toImmutableDruidServer(),
                        new TestLoadQueuePeon()
                    )
                )
                .add(
                    new ServerHolder(
                        new DruidServer("histT2", "histT2", null, 100L, ServerType.HISTORICAL, Tier.T2, 1)
                            .addDataSegment(segmentWiki).addDataSegment(segmentKoala).addDataSegment(broadcastSegment)
                            .toImmutableDruidServer(),
                        new TestLoadQueuePeon()
                    )
                )
                .add(
                    new ServerHolder(
                        new DruidServer("broker1", "broker1", null, 100L, ServerType.BROKER, Tier.T1, 1)
                            .addDataSegment(segmentWiki).addDataSegment(segmentKoala).addDataSegment(broadcastSegment)
                            .toImmutableDruidServer(),
                        new TestLoadQueuePeon()
                    )
                )
                .add(
                    new ServerHolder(
                        new DruidServer("indexer1", "indexer1", null, 100L, ServerType.INDEXER_EXECUTOR, Tier.T1, 1)
                            .addDataSegment(realtimeSegment)
                            .toImmutableDruidServer(),
                        new TestLoadQueuePeon()
                    )
                )
                .build()
        )
        .withUsedSegments(usedSegments)
        .withBroadcastDatasources(Collections.singleton(DS.BROADCAST))
        .withDatabaseRuleManager(databaseRuleManager)
        .build();

    params = new UnloadUnusedSegments(loadQueueManager).run(params);
    CoordinatorRunStats stats = params.getCoordinatorStats();

    // We drop segment1 and broadcast1 from all servers, realtimeSegment is not dropped by the indexer
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.UNNEEDED, Tier.T1, DS.WIKI));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.UNNEEDED, Tier.T2, DS.WIKI));

    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.UNNEEDED, Tier.T1, DS.BROADCAST));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.UNNEEDED, Tier.T2, DS.BROADCAST));
  }

  private void setupRuleManager()
  {
    final LoadRule loadOnT1T2 = new ForeverLoadRule(ImmutableMap.of(Tier.T1, 1, Tier.T2, 1), true);
    databaseRuleManager.overrideRule(DS.WIKI, Collections.singletonList(loadOnT1T2), null);
    databaseRuleManager.overrideRule(DS.KOALA, Collections.singletonList(loadOnT1T2), null);

    databaseRuleManager.overrideRule(
        DS.BROADCAST,
        Collections.singletonList(new ForeverBroadcastDistributionRule()),
        null
    );
  }
}
