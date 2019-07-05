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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BroadcastDistributionRuleTest
{
  private DruidCluster druidCluster;
  private ServerHolder holderOfSmallSegment;
  private List<ServerHolder> holdersOfLargeSegments = new ArrayList<>();
  private List<ServerHolder> holdersOfLargeSegments2 = new ArrayList<>();
  private final List<DataSegment> largeSegments = new ArrayList<>();
  private final List<DataSegment> largeSegments2 = new ArrayList<>();
  private DataSegment smallSegment;
  private DruidCluster secondCluster;
  private ServerHolder activeServer;
  private ServerHolder decommissioningServer1;
  private ServerHolder decommissioningServer2;

  @Before
  public void setUp()
  {
    smallSegment = new DataSegment(
        "small_source",
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
            "hot",
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
                "hot",
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
                DruidServer.DEFAULT_TIER,
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
                DruidServer.DEFAULT_TIER,
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
                "hot",
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
                DruidServer.DEFAULT_TIER,
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
            "tier1",
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
            "tier1",
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
            "tier1",
            0
        ).addDataSegment(largeSegments.get(1))
         .toImmutableDruidServer(),
        new LoadQueuePeonTester(),
        true
    );

    druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                holdersOfLargeSegments.get(0),
                holderOfSmallSegment,
                holdersOfLargeSegments2.get(0)
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder()))),
            DruidServer.DEFAULT_TIER,
            Stream.of(
                holdersOfLargeSegments.get(1),
                holdersOfLargeSegments.get(2),
                holdersOfLargeSegments2.get(1)
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    secondCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "tier1",
            Stream.of(
                activeServer,
                decommissioningServer1,
                decommissioningServer2
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );
  }

  @Test
  public void testBroadcastToSingleDataSource()
  {
    final ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule(ImmutableList.of("large_source"));

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegmentsInTest(
                                         smallSegment,
                                         largeSegments.get(0),
                                         largeSegments.get(1),
                                         largeSegments.get(2),
                                         largeSegments2.get(0),
                                         largeSegments2.get(1)
                                     ).build(),
        smallSegment
    );

    assertEquals(3L, stats.getGlobalStat(LoadRule.ASSIGNED_COUNT));
    assertEquals(false, stats.hasPerTierStats());

    assertTrue(
        holdersOfLargeSegments.stream()
                              .allMatch(holder -> holder.getPeon().getSegmentsToLoad().contains(smallSegment))
    );

    assertTrue(
        holdersOfLargeSegments2.stream()
                               .noneMatch(holder -> holder.getPeon().getSegmentsToLoad().contains(smallSegment))
    );

    assertFalse(holderOfSmallSegment.getPeon().getSegmentsToLoad().contains(smallSegment));
  }

  /**
   * Servers:
   * name             | segments
   * -----------------+--------------
   * active           | large segment
   * decommissioning1 | small segment
   * decommissioning2 | large segment
   *
   * After running the rule for the small segment:
   * active           | large & small segments
   * decommissioning1 |
   * decommissionint2 | large segment
   */
  @Test
  public void testBroadcastDecommissioning()
  {
    final ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule(ImmutableList.of("large_source"));

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(secondCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(secondCluster))
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegmentsInTest(
                                         smallSegment,
                                         largeSegments.get(0),
                                         largeSegments.get(1)
                                     ).build(),
        smallSegment
    );

    assertEquals(1L, stats.getGlobalStat(LoadRule.ASSIGNED_COUNT));
    assertEquals(false, stats.hasPerTierStats());

    assertEquals(1, activeServer.getPeon().getSegmentsToLoad().size());
    assertEquals(1, decommissioningServer1.getPeon().getSegmentsToDrop().size());
    assertEquals(0, decommissioningServer2.getPeon().getSegmentsToLoad().size());
  }

  @Test
  public void testBroadcastToMultipleDataSources()
  {
    final ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule(
        ImmutableList.of("large_source", "large_source2")
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegmentsInTest(
                                         smallSegment,
                                         largeSegments.get(0),
                                         largeSegments.get(1),
                                         largeSegments.get(2),
                                         largeSegments2.get(0),
                                         largeSegments2.get(1)
                                     ).build(),
        smallSegment
    );

    assertEquals(5L, stats.getGlobalStat(LoadRule.ASSIGNED_COUNT));
    assertEquals(false, stats.hasPerTierStats());

    assertTrue(
        holdersOfLargeSegments.stream()
                              .allMatch(holder -> holder.getPeon().getSegmentsToLoad().contains(smallSegment))
    );

    assertTrue(
        holdersOfLargeSegments2.stream()
                               .allMatch(holder -> holder.getPeon().getSegmentsToLoad().contains(smallSegment))
    );

    assertFalse(holderOfSmallSegment.getPeon().getSegmentsToLoad().contains(smallSegment));
  }

  @Test
  public void testBroadcastToAllServers()
  {
    final ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule(null);

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegmentsInTest(
                                         smallSegment,
                                         largeSegments.get(0),
                                         largeSegments.get(1),
                                         largeSegments.get(2),
                                         largeSegments2.get(0),
                                         largeSegments2.get(1)
                                     ).build(),
        smallSegment
    );

    assertEquals(6L, stats.getGlobalStat(LoadRule.ASSIGNED_COUNT));
    assertEquals(false, stats.hasPerTierStats());

    assertTrue(
        druidCluster.getAllServers().stream()
                    .allMatch(holder -> holder.getPeon().getSegmentsToLoad().contains(smallSegment))
    );
  }
}
