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

package org.apache.druid.server.coordinator.loading;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for version-aware segment assignment in {@link StrategicSegmentAssigner}.
 */
@RunWith(Parameterized.class)
public class StrategicSegmentAssignerTest
{
  private static final String TIER = "tier1";
  private static final String VERSION_RED = "red";
  private static final String VERSION_BLUE = "blue";

  private final boolean useRoundRobinAssignment;
  private final AtomicInteger serverId = new AtomicInteger();

  private SegmentLoadQueueManager loadQueueManager;
  private ListeningExecutorService exec;
  private CostBalancerStrategy balancerStrategy;

  @Parameterized.Parameters(name = "useRoundRobin = {0}")
  public static List<Boolean> getTestParams()
  {
    return Arrays.asList(true, false);
  }

  public StrategicSegmentAssignerTest(boolean useRoundRobinAssignment)
  {
    this.useRoundRobinAssignment = useRoundRobinAssignment;
  }

  @Before
  public void setUp()
  {
    loadQueueManager = new SegmentLoadQueueManager(null, null);
    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "StrategicSegmentAssignerTest-%d"));
    balancerStrategy = new CostBalancerStrategy(exec);
  }

  @After
  public void tearDown()
  {
    exec.shutdown();
  }

  @Test
  public void testSingleVersionTier_noCoordinatingVersions_behaviorUnchanged()
  {
    final DataSegment segment = createSegment();
    final ServerHolder server1 = createServer(TIER, null);
    final ServerHolder server2 = createServer(TIER, null);
    DruidCluster cluster = DruidCluster.builder().addTier(TIER, server1, server2).build();

    CoordinatorRunStats stats = runRule(
        loadForever(ImmutableMap.of(TIER, 1)),
        segment,
        cluster,
        Collections.emptySet(),
        segment
    );

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER, TestDataSource.WIKI));
    Assert.assertEquals(1, server1.getLoadingSegments().size() + server2.getLoadingSegments().size());
  }

  @Test
  public void testTwoVersionTier_eachVersionGetsRequiredReplicas()
  {
    // Two versions with two servers each; rule requires 1 replica in the tier.
    // With coordinatingVersions active, each version should receive 1 replica independently.
    final DataSegment segment = createSegment();
    final ServerHolder redServer1 = createServer(TIER, VERSION_RED);
    final ServerHolder redServer2 = createServer(TIER, VERSION_RED);
    final ServerHolder blueServer1 = createServer(TIER, VERSION_BLUE);
    final ServerHolder blueServer2 = createServer(TIER, VERSION_BLUE);
    DruidCluster cluster = DruidCluster
        .builder()
        .addTier(TIER, redServer1, redServer2, blueServer1, blueServer2)
        .build();

    CoordinatorRunStats stats = runRule(
        loadForever(ImmutableMap.of(TIER, 1)),
        segment,
        cluster,
        Set.of(VERSION_RED, VERSION_BLUE),
        segment
    );

    // 1 replica per version = 2 total assignments, both reported under the same tier.
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER, TestDataSource.WIKI));
  }

  @Test
  public void testTwoVersionTier_segmentAlreadyLoadedInBothVersions_noAdditionalAssignment()
  {
    final DataSegment segment = createSegment();
    final ServerHolder redServer = createServer(TIER, VERSION_RED, segment);
    final ServerHolder blueServer = createServer(TIER, VERSION_BLUE, segment);
    DruidCluster cluster = DruidCluster.builder().addTier(TIER, redServer, blueServer).build();

    CoordinatorRunStats stats = runRule(
        loadForever(ImmutableMap.of(TIER, 1)),
        segment,
        cluster,
        Set.of(VERSION_RED, VERSION_BLUE),
        segment
    );

    Assert.assertFalse(stats.hasStat(Stats.Segments.ASSIGNED));
  }

  @Test
  public void testTwoVersionTier_segmentLoadedOnlyInOneVersion_assignsToMissingVersion()
  {
    final DataSegment segment = createSegment();
    // Red already has the segment; blue does not.
    final ServerHolder redServer = createServer(TIER, VERSION_RED, segment);
    final ServerHolder blueServer = createServer(TIER, VERSION_BLUE);
    DruidCluster cluster = DruidCluster.builder().addTier(TIER, redServer, blueServer).build();

    CoordinatorRunStats stats = runRule(
        loadForever(ImmutableMap.of(TIER, 1)),
        segment,
        cluster,
        Set.of(VERSION_RED, VERSION_BLUE),
        segment
    );

    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER, TestDataSource.WIKI));
    Assert.assertEquals(0, redServer.getLoadingSegments().size());
    Assert.assertEquals(1, blueServer.getLoadingSegments().size());
  }

  @Test
  public void testTwoVersionTier_onlyOneVersionInCoordinatingVersions_tierWideBehaviorForOther()
  {
    // Only "red" is in coordinatingVersions. "blue" servers exist but are not coordinated,
    // so the tier falls back to a single tier-wide replica count of 1.
    final DataSegment segment = createSegment();
    final ServerHolder redServer = createServer(TIER, VERSION_RED);
    final ServerHolder blueServer = createServer(TIER, VERSION_BLUE);
    DruidCluster cluster = DruidCluster.builder().addTier(TIER, redServer, blueServer).build();

    CoordinatorRunStats stats = runRule(
        loadForever(ImmutableMap.of(TIER, 1)),
        segment,
        cluster,
        Set.of(VERSION_RED),   // only red is coordinated; intersection has 1 entry, no multi-version expansion
        segment
    );

    // Intersection of coordinatingVersions and tier versions yields only {"red"}, which is a single
    // version, same as the tier-wide path. One replica is assigned across the whole tier.
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.ASSIGNED, TIER, TestDataSource.WIKI));
  }

  private CoordinatorRunStats runRule(
      LoadRule rule,
      DataSegment segment,
      DruidCluster cluster,
      Set<String> coordinatingVersions,
      DataSegment... usedSegments
  )
  {
    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .builder()
        .withDruidCluster(cluster)
        .withBalancerStrategy(balancerStrategy)
        .withUsedSegments(usedSegments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withSmartSegmentLoading(false)
                                    .withUseRoundRobinSegmentAssignment(useRoundRobinAssignment)
                                    .withCoordinatingVersions(coordinatingVersions)
                                    .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    rule.run(segment, params.getSegmentAssigner());
    return params.getCoordinatorStats();
  }

  private ServerHolder createServer(String tier, String version, DataSegment... loadedSegments)
  {
    final int id = serverId.incrementAndGet();
    final String name = "hist_" + tier + "_" + id;
    DruidServer server = new DruidServer(
        new DruidServerMetadata(name, name, null, 10L << 30, null, ServerType.HISTORICAL, tier, 0, version)
    );
    for (DataSegment segment : loadedSegments) {
      server.addDataSegment(segment);
    }
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  private static LoadRule loadForever(Map<String, Integer> tieredReplicants)
  {
    return new ForeverLoadRule(tieredReplicants, null);
  }

  private static DataSegment createSegment()
  {
    final SegmentId segmentId = SegmentId.of(
        TestDataSource.WIKI,
        Intervals.of("2024-01-01/2024-01-02"),
        "1",
        NoneShardSpec.instance()
    );
    return DataSegment.builder(segmentId)
                      .shardSpec(NoneShardSpec.instance())
                      .size(100)
                      .build();
  }
}
