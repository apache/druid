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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.loadqueue.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.loadqueue.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BroadcastDistributionRuleTest
{
  private DruidCluster druidCluster;
  private ServerHolder holderOfSmallSegment;
  private final List<ServerHolder> holdersOfLargeSegments = new ArrayList<>();
  private final List<ServerHolder> holdersOfLargeSegments2 = new ArrayList<>();
  private final List<DataSegment> largeSegments = new ArrayList<>();
  private final List<DataSegment> largeSegments2 = new ArrayList<>();
  private DataSegment smallSegment;
  private DruidCluster secondCluster;
  private ServerHolder activeServer;
  private ServerHolder decommissioningServer1;
  private ServerHolder decommissioningServer2;
  private SegmentLoadQueueManager stateManager;

  private static final String DS_SMALL = "small_source";
  private static final String TIER_1 = "tier1";
  private static final String TIER_2 = "tier2";

  @Before
  public void setUp()
  {
    stateManager = new SegmentLoadQueueManager(null, null, null);
    smallSegment = new DataSegment(
        DS_SMALL,
        Intervals.of("0/1000"),
        DateTimes.nowUtc().toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        0
    );

    for (int i = 0; i < 3; i++) {
      largeSegments.add(
          new DataSegment(
              "large_source",
              Intervals.of((i * 1000) + "/" + ((i + 1) * 1000)),
              DateTimes.nowUtc().toString(),
              new HashMap<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              NoneShardSpec.instance(),
              0,
              100
          )
      );
    }

    for (int i = 0; i < 2; i++) {
      largeSegments2.add(
          new DataSegment(
              "large_source2",
              Intervals.of((i * 1000) + "/" + ((i + 1) * 1000)),
              DateTimes.nowUtc().toString(),
              new HashMap<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              NoneShardSpec.instance(),
              0,
              100
          )
      );
    }

    holderOfSmallSegment = new ServerHolder(
        new DruidServer(
            "serverHot2",
            "hostHot2",
            null,
            1000,
            ServerType.HISTORICAL,
            TIER_1,
            0
        ).addDataSegment(smallSegment)
         .toImmutableDruidServer(),
        new LoadQueuePeonTester()
    );

    holdersOfLargeSegments.add(
        new ServerHolder(
            new DruidServer(
                "serverHot1",
                "hostHot1",
                null,
                1000,
                ServerType.HISTORICAL,
                TIER_1,
                0
            ).addDataSegment(largeSegments.get(0))
             .toImmutableDruidServer(),
            new LoadQueuePeonTester()
        )
    );
    holdersOfLargeSegments.add(
        new ServerHolder(
            new DruidServer(
                "serverNorm1",
                "hostNorm1",
                null,
                1000,
                ServerType.HISTORICAL,
                TIER_2,
                0
            ).addDataSegment(largeSegments.get(1))
             .toImmutableDruidServer(),
            new LoadQueuePeonTester()
        )
    );
    holdersOfLargeSegments.add(
        new ServerHolder(
            new DruidServer(
                "serverNorm2",
                "hostNorm2",
                null,
                100,
                ServerType.HISTORICAL,
                TIER_2,
                0
            ).addDataSegment(largeSegments.get(2))
             .toImmutableDruidServer(),
            new LoadQueuePeonTester()
        )
    );

    holdersOfLargeSegments2.add(
        new ServerHolder(
            new DruidServer(
                "serverHot3",
                "hostHot3",
                null,
                1000,
                ServerType.HISTORICAL,
                TIER_1,
                0
            ).addDataSegment(largeSegments2.get(0))
             .toImmutableDruidServer(),
            new LoadQueuePeonTester()
        )
    );
    holdersOfLargeSegments2.add(
        new ServerHolder(
            new DruidServer(
                "serverNorm3",
                "hostNorm3",
                null,
                100,
                ServerType.HISTORICAL,
                TIER_2,
                0
            ).addDataSegment(largeSegments2.get(1))
             .toImmutableDruidServer(),
            new LoadQueuePeonTester()
        )
    );

    activeServer = new ServerHolder(
        new DruidServer(
            "active",
            "host1",
            null,
            100,
            ServerType.HISTORICAL,
            TIER_1,
            0
        ).addDataSegment(largeSegments.get(0))
         .toImmutableDruidServer(),
        new LoadQueuePeonTester()
    );

    decommissioningServer1 = new ServerHolder(
        new DruidServer(
            "decommissioning1",
            "host2",
            null,
            100,
            ServerType.HISTORICAL,
            TIER_1,
            0
        ).addDataSegment(smallSegment)
         .toImmutableDruidServer(),
        new LoadQueuePeonTester(),
        true
    );

    decommissioningServer2 = new ServerHolder(
        new DruidServer(
            "decommissioning2",
            "host3",
            null,
            100,
            ServerType.HISTORICAL,
            TIER_1,
            0
        ).addDataSegment(largeSegments.get(1))
         .toImmutableDruidServer(),
        new LoadQueuePeonTester(),
        true
    );

    druidCluster = DruidCluster
        .builder()
        .addTier(
            TIER_1,
            holdersOfLargeSegments.get(0),
            holderOfSmallSegment,
            holdersOfLargeSegments2.get(0)
        )
        .addTier(
            TIER_2,
            holdersOfLargeSegments.get(1),
            holdersOfLargeSegments.get(2),
            holdersOfLargeSegments2.get(1)
        )
        .build();

    secondCluster = DruidCluster
        .builder()
        .addTier(
            TIER_1,
            activeServer,
            decommissioningServer1,
            decommissioningServer2
        )
        .build();
  }

  @Test
  public void testBroadcastToSingleDataSource()
  {
    final ForeverBroadcastDistributionRule rule =
        new ForeverBroadcastDistributionRule();

    CoordinatorRunStats stats = runRuleAndGetStats(
        rule,
        smallSegment,
        makeCoordinartorRuntimeParams(
            druidCluster,
            smallSegment,
            largeSegments.get(0),
            largeSegments.get(1),
            largeSegments.get(2),
            largeSegments2.get(0),
            largeSegments2.get(1)
        )
    );

    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.ASSIGNED_BROADCAST, TIER_1, DS_SMALL));
    Assert.assertEquals(3L, stats.getSegmentStat(Stats.Segments.ASSIGNED_BROADCAST, TIER_2, DS_SMALL));

    Assert.assertTrue(
        holdersOfLargeSegments.stream().allMatch(
            holder -> holder.isLoadingSegment(smallSegment)
        )
    );
    Assert.assertTrue(
        holdersOfLargeSegments2.stream().allMatch(
            holder -> holder.isLoadingSegment(smallSegment)
        )
    );
    Assert.assertTrue(holderOfSmallSegment.isServingSegment(smallSegment));
  }

  private static DruidCoordinatorRuntimeParams makeCoordinartorRuntimeParams(
      DruidCluster druidCluster,
      DataSegment... usedSegments
  )
  {
    return CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(druidCluster)
        .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster, false))
        .withUsedSegmentsInTest(usedSegments)
        .build();
  }

  /**
   * Servers:
   * name             | segments
   * -----------------+--------------
   * active           | large segment
   * decommissioning1 | small segment
   * decommissioning2 | large segment
   * <p>
   * After running the rule for the small segment:
   * active           | large & small segments
   * decommissioning1 |
   * decommissionint2 | large segment
   */
  @Test
  public void testBroadcastDecommissioning()
  {
    final ForeverBroadcastDistributionRule rule =
        new ForeverBroadcastDistributionRule();

    CoordinatorRunStats stats = runRuleAndGetStats(
        rule,
        smallSegment,
        makeCoordinartorRuntimeParams(
            secondCluster,
            smallSegment,
            largeSegments.get(0),
            largeSegments.get(1)
        )
    );

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED_BROADCAST, TIER_1, DS_SMALL));
    Assert.assertEquals(1, activeServer.getPeon().getSegmentsToLoad().size());
    Assert.assertEquals(1, decommissioningServer1.getPeon().getSegmentsToDrop().size());
    Assert.assertEquals(0, decommissioningServer2.getPeon().getSegmentsToLoad().size());
  }

  @Test
  public void testBroadcastToMultipleDataSources()
  {
    final ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();

    CoordinatorRunStats stats = runRuleAndGetStats(
        rule,
        smallSegment,
        makeCoordinartorRuntimeParams(
            druidCluster,
            smallSegment,
            largeSegments.get(0),
            largeSegments.get(1),
            largeSegments.get(2),
            largeSegments2.get(0),
            largeSegments2.get(1)
        )
    );

    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.ASSIGNED_BROADCAST, TIER_1, DS_SMALL));
    Assert.assertEquals(3L, stats.getSegmentStat(Stats.Segments.ASSIGNED_BROADCAST, TIER_2, DS_SMALL));

    Assert.assertTrue(
        holdersOfLargeSegments.stream().allMatch(
            holder -> holder.isLoadingSegment(smallSegment)
        )
    );
    Assert.assertTrue(
        holdersOfLargeSegments2.stream().allMatch(
            holder -> holder.isLoadingSegment(smallSegment)
        )
    );
    Assert.assertFalse(holderOfSmallSegment.isLoadingSegment(smallSegment));
  }

  @Test
  public void testBroadcastToAllServers()
  {
    final ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();

    CoordinatorRunStats stats = runRuleAndGetStats(
        rule,
        smallSegment,
        makeCoordinartorRuntimeParams(
            druidCluster,
            smallSegment,
            largeSegments.get(0),
            largeSegments.get(1),
            largeSegments.get(2),
            largeSegments2.get(0),
            largeSegments2.get(1)
        )
    );

    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.ASSIGNED_BROADCAST, TIER_1, DS_SMALL));
    Assert.assertEquals(3L, stats.getSegmentStat(Stats.Segments.ASSIGNED_BROADCAST, TIER_2, DS_SMALL));

    Assert.assertTrue(
        druidCluster.getAllServers().stream().allMatch(
            holder -> holder.isLoadingSegment(smallSegment) || holder.isServingSegment(smallSegment)
        )
    );
  }

  private CoordinatorRunStats runRuleAndGetStats(
      Rule rule,
      DataSegment segment,
      DruidCoordinatorRuntimeParams params
  )
  {
    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    ReplicationThrottler throttler = new ReplicationThrottler(
        null,
        dynamicConfig.getReplicationThrottleLimit(),
        dynamicConfig.getMaxNonPrimaryReplicantsToLoad()
    );
    StrategicSegmentAssigner segmentAssigner = new StrategicSegmentAssigner(
        stateManager,
        params.getDruidCluster(),
        params.getSegmentReplicantLookup(),
        throttler,
        params.getBalancerStrategy(),
        dynamicConfig
    );
    rule.run(segment, segmentAssigner);
    return segmentAssigner.getStats();
  }
}
