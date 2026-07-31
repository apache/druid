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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.rules.CannotMatchBehavior;
import org.apache.druid.server.coordinator.rules.ExactProjectionPartialLoadMatcher;
import org.apache.druid.server.coordinator.rules.ForeverPartialLoadRule;
import org.apache.druid.server.coordinator.rules.PartialLoadRule;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that {@link StrategicSegmentAssigner#replicateSegmentPartially} threads the {@link PartialLoadProfile}
 * through the load queue and reconciles fingerprint state correctly: matching replicas count toward the requirement,
 * stale-fingerprint replicas (and full-load replicas under a partial rule) are treated as "loaded but not satisfying"
 * and follow the load-then-drop swap so the cluster never goes unavailable during reconciliation.
 */
public class StrategicSegmentAssignerPartialTest
{
  private static final String TIER1 = "tier1";
  private static final String TIER2 = "tier2";

  private static final String FP_REVENUE = "v1:deadbeefcafebabe";
  private static final String FP_USERS = "v1:0123456789abcdef";

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;
  private SegmentLoadQueueManager loadQueueManager;

  private final AtomicInteger serverId = new AtomicInteger();

  @Before
  public void setUp()
  {
    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "StrategicSegmentAssignerPartialTest-%d"));
    balancerStrategy = new CostBalancerStrategy(exec);
    loadQueueManager = new SegmentLoadQueueManager(null, null);
  }

  @After
  public void tearDown()
  {
    exec.shutdown();
  }

  @Test
  public void testReplicateSegmentPartiallyAssignsAndThreadsProfileToPeon()
  {
    final ServerHolder server = createServer(TIER1);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, server).build();

    final DataSegment segment = createSegment();
    final PartialLoadProfile profile = profileForRevenue();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner().replicateSegmentPartially(segment, profile, ImmutableMap.of(TIER1, 1));

    Assert.assertEquals(
        1L,
        params.getCoordinatorStats().getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER1, segment.getDataSource())
    );
    Assert.assertTrue(server.getLoadingSegments().contains(segment));
    Assert.assertEquals(profile, ((TestLoadQueuePeon) server.getPeon()).getProfileFor(segment));
  }

  @Test
  public void testReplicateSegmentPartiallyAssignsAcrossTiers()
  {
    final ServerHolder s1 = createServer(TIER1);
    final ServerHolder s2 = createServer(TIER2);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, s1).addTier(TIER2, s2).build();

    final DataSegment segment = createSegment();
    final PartialLoadProfile profile = profileForRevenue();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner()
          .replicateSegmentPartially(segment, profile, ImmutableMap.of(TIER1, 1, TIER2, 1));

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER1, segment.getDataSource()));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER2, segment.getDataSource()));
    Assert.assertEquals(profile, ((TestLoadQueuePeon) s1.getPeon()).getProfileFor(segment));
    Assert.assertEquals(profile, ((TestLoadQueuePeon) s2.getPeon()).getProfileFor(segment));
  }

  @Test
  public void testNoActionWhenMatchingReplicaAlreadyLoaded()
  {
    // s1 has the segment loaded with the matching fingerprint; required = 1. Reconciler should do nothing; no
    // load queued on s2, no drop on s1.
    final DataSegment segment = createSegment();
    final ServerHolder s1 = createServerWithLoaded(TIER1, segment, profileForRevenue());
    final ServerHolder s2 = createServer(TIER1);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, s1, s2).build();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner()
          .replicateSegmentPartially(segment, profileForRevenue(), ImmutableMap.of(TIER1, 1));

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertFalse(stats.hasStat(Stats.Segments.PARTIAL_ASSIGNED));
    Assert.assertFalse(stats.hasStat(Stats.Segments.PARTIAL_STALE_DROPPED));
    Assert.assertFalse(stats.hasStat(Stats.Segments.DROPPED));
    Assert.assertTrue(s2.getLoadingSegments().isEmpty());
    Assert.assertTrue(s1.getPeon().getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testFullFallbackLoadedBytesCountsAsMatching()
  {
    // s1 announced with a loaded profile whose loadedBytes equals the full segment size (historical was asked to
    // partial-load but fell back to full). The fingerprint matches the rule, so the rule is satisfied; no further
    // action.
    final DataSegment segment = createSegment();
    final PartialLoadProfile fullSizeLoaded = PartialLoadProfile.forLoaded(
        profileForRevenue().wrappedLoadSpec(),
        FP_REVENUE,
        segment.getSize()
    );
    final ServerHolder s1 = createServerWithLoaded(TIER1, segment, fullSizeLoaded);
    final ServerHolder s2 = createServer(TIER1);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, s1, s2).build();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner()
          .replicateSegmentPartially(segment, profileForRevenue(), ImmutableMap.of(TIER1, 1));

    Assert.assertFalse(params.getCoordinatorStats().hasStat(Stats.Segments.PARTIAL_ASSIGNED));
    Assert.assertTrue(s2.getLoadingSegments().isEmpty());
  }

  @Test
  public void testFullLoadReplicaTreatedAsStaleAgainstPartialRule()
  {
    // s1 holds the segment as a regular full-load (no profile). Under a partial rule it counts as stale: queue a
    // fresh load to satisfy the partial rule, but don't drop s1 yet; stale stays serving until the matching load
    // completes. Note: s1 is also reload-eligible (additive), but because s2 is empty, s2 is preferred.
    final DataSegment segment = createSegment();
    final ServerHolder s1 = createServerWithLoaded(TIER1, segment, null);
    final ServerHolder s2 = createServer(TIER1);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, s1, s2).build();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner()
          .replicateSegmentPartially(segment, profileForRevenue(), ImmutableMap.of(TIER1, 1));

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER1, segment.getDataSource()));
    Assert.assertFalse(
        "Stale must not be dropped before matching has actually loaded",
        stats.hasStat(Stats.Segments.PARTIAL_STALE_DROPPED)
    );
    Assert.assertTrue(s2.getLoadingSegments().contains(segment));
    Assert.assertEquals(profileForRevenue(), ((TestLoadQueuePeon) s2.getPeon()).getProfileFor(segment));
    Assert.assertTrue(s1.getPeon().getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testStaleDroppedAfterMatchingLoadedSatisfiesRequirement()
  {
    // s1 has matching fingerprint already loaded (sufficient to satisfy required=1). s2 holds the segment as
    // full-load (stale). Reconciler should drop s2 in this run because s1's matching count already covers the
    // requirement.
    final DataSegment segment = createSegment();
    final ServerHolder s1 = createServerWithLoaded(TIER1, segment, profileForRevenue());
    final ServerHolder s2 = createServerWithLoaded(TIER1, segment, null);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, s1, s2).build();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner()
          .replicateSegmentPartially(segment, profileForRevenue(), ImmutableMap.of(TIER1, 1));

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.PARTIAL_STALE_DROPPED, TIER1, segment.getDataSource()));
    Assert.assertFalse(stats.hasStat(Stats.Segments.PARTIAL_ASSIGNED));
    Assert.assertTrue(s2.getPeon().getSegmentsToDrop().contains(segment));
    Assert.assertTrue(s1.getPeon().getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testStuckStateAdditiveReloadOnStaleServers()
  {
    // Both servers hold the segment with mismatched fingerprints (and there are no spare servers). Reconciler
    // queues additive-reload requests on both stale servers; the historical's additive-load semantics fill in the
    // missing parts in place. matching count is still 0 in the same run, so stale isn't dropped; but next run, after
    // the loads land, the servers reclassify as matching.
    final DataSegment segment = createSegment();
    final PartialLoadProfile usersProfile = PartialLoadProfile.forLoaded(
        Map.of("type", "partialProjection", "projections", List.of("users"), "fingerprint", FP_USERS),
        FP_USERS,
        512L
    );
    final ServerHolder s1 = createServerWithLoaded(TIER1, segment, usersProfile);
    final ServerHolder s2 = createServerWithLoaded(TIER1, segment, usersProfile);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, s1, s2).build();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner()
          .replicateSegmentPartially(segment, profileForRevenue(), ImmutableMap.of(TIER1, 2));

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER1, segment.getDataSource()));
    Assert.assertEquals(profileForRevenue(), ((TestLoadQueuePeon) s1.getPeon()).getProfileFor(segment));
    Assert.assertEquals(profileForRevenue(), ((TestLoadQueuePeon) s2.getPeon()).getProfileFor(segment));
    // Stale must NOT be dropped; matching hasn't loaded yet.
    Assert.assertFalse(stats.hasStat(Stats.Segments.PARTIAL_STALE_DROPPED));
  }

  @Test
  public void testDropsExcessMatchingReplicas()
  {
    // 2 matching replicas, required = 1. Reconciler drops the excess matching using the regular DROPPED stat (this
    // is the surplus path, not the stale path).
    final DataSegment segment = createSegment();
    final ServerHolder s1 = createServerWithLoaded(TIER1, segment, profileForRevenue());
    final ServerHolder s2 = createServerWithLoaded(TIER1, segment, profileForRevenue());
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, s1, s2).build();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner()
          .replicateSegmentPartially(segment, profileForRevenue(), ImmutableMap.of(TIER1, 1));

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.DROPPED, TIER1, segment.getDataSource()));
    Assert.assertEquals(
        1,
        s1.getPeon().getSegmentsToDrop().size() + s2.getPeon().getSegmentsToDrop().size()
    );
  }

  @Test
  public void testReplicateSegmentPartiallySkipsDecommissioningServer()
  {
    // Decommissioning server must not be picked as a load destination, just like the regular full-load path.
    final ServerHolder decommServer = createDecommissioningServer(TIER1);
    final ServerHolder activeServer = createServer(TIER1);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, decommServer, activeServer).build();

    final DataSegment segment = createSegment();
    final PartialLoadProfile profile = profileForRevenue();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner().replicateSegmentPartially(segment, profile, ImmutableMap.of(TIER1, 1));

    Assert.assertEquals(
        1L,
        params.getCoordinatorStats().getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER1, segment.getDataSource())
    );
    Assert.assertTrue(decommServer.getLoadingSegments().isEmpty());
    Assert.assertTrue(activeServer.getLoadingSegments().contains(segment));
    Assert.assertEquals(profile, ((TestLoadQueuePeon) activeServer.getPeon()).getProfileFor(segment));
    Assert.assertNull(((TestLoadQueuePeon) decommServer.getPeon()).getProfileFor(segment));
  }

  @Test
  public void testStaleInFlightCancelledAndReplaced()
  {
    // s1 has a stale-fingerprint load already in-flight (from a previous coordinator run with a different rule).
    // The current rule wants the revenue fingerprint. Reconciler cancels the stale in-flight on s1 and queues a
    // matching load (additive; same server, same slot).
    final DataSegment segment = createSegment();
    final PartialLoadProfile staleInFlightProfile = PartialLoadProfile.forRequest(
        Map.of(
            "type", "partialProjection",
            "projections", List.of("users"),
            "fingerprint", FP_USERS
        ),
        FP_USERS
    );
    final TestLoadQueuePeon peon = new TestLoadQueuePeon();
    // Pre-seed the peon with an in-flight stale load; simulates a previous coordinator run that queued the load
    // before the rule changed.
    peon.addInFlightHolder(new SegmentHolder(
        segment,
        SegmentAction.LOAD,
        staleInFlightProfile,
        org.joda.time.Duration.standardSeconds(10),
        null
    ));
    final DruidServer druidServer = createDruidServer(TIER1);
    final ServerHolder s1 = new ServerHolder(druidServer.toImmutableDruidServer(), peon);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, s1).build();

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    params.getSegmentAssigner()
          .replicateSegmentPartially(segment, profileForRevenue(), ImmutableMap.of(TIER1, 1));

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertEquals(
        1L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_STALE_CANCELLED, TIER1, segment.getDataSource())
    );
    Assert.assertEquals(
        1L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER1, segment.getDataSource())
    );
    Assert.assertEquals(profileForRevenue(), peon.getProfileFor(segment));
  }

  @Test
  public void testForeverPartialLoadRuleEndToEndThreadsProfile()
  {
    // Run the full rule.run() → assigner.replicateSegmentPartially() → peon path with a real matcher that resolves
    // to a wrapped projection load spec on a segment carrying the matching projection.
    final ServerHolder server = createServer(TIER1);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, server).build();

    final DataSegment segment = segmentWithProjections(List.of("revenue", "users"));
    final PartialLoadRule rule = new ForeverPartialLoadRule(
        ImmutableMap.of(TIER1, 1),
        null,
        new ExactProjectionPartialLoadMatcher(List.of("revenue")),
        null
    );

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    rule.run(segment, params.getSegmentAssigner());

    Assert.assertEquals(
        1L,
        params.getCoordinatorStats().getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER1, segment.getDataSource())
    );
    final PartialLoadProfile profile = ((TestLoadQueuePeon) server.getPeon()).getProfileFor(segment);
    Assert.assertNotNull("Matcher matched, profile should be threaded", profile);
    Assert.assertEquals("partialProjection", profile.wrappedLoadSpec().get("type"));
    Assert.assertEquals(List.of("revenue"), profile.wrappedLoadSpec().get("projections"));
    Assert.assertTrue(profile.fingerprint().startsWith("v1:"));
    Assert.assertNull("Outbound request profile should not carry loadedBytes", profile.loadedBytes());
  }

  @Test
  public void testForeverPartialLoadRuleEndToEndFullLoadFallback()
  {
    // Matcher does not apply (segment has no overlap); FULL_LOAD onCannotMatch (default) → run() routes through
    // replicateSegment instead, so the peon must not see a profile and the stat is the regular ASSIGNED.
    final ServerHolder server = createServer(TIER1);
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER1, server).build();

    final DataSegment segment = segmentWithProjections(List.of("users"));
    final PartialLoadRule rule = new ForeverPartialLoadRule(
        ImmutableMap.of(TIER1, 1),
        null,
        new ExactProjectionPartialLoadMatcher(List.of("nonexistent")),
        CannotMatchBehavior.FULL_LOAD
    );

    final DruidCoordinatorRuntimeParams params = makeRuntimeParams(cluster, segment);
    rule.run(segment, params.getSegmentAssigner());

    Assert.assertEquals(
        1L,
        params.getCoordinatorStats().getSegmentStat(Stats.Segments.ASSIGNED, TIER1, segment.getDataSource())
    );
    Assert.assertTrue(server.getLoadingSegments().contains(segment));
    Assert.assertNull(
        "Full-load fallback must not thread a profile to the peon",
        ((TestLoadQueuePeon) server.getPeon()).getProfileFor(segment)
    );
  }

  private DruidCoordinatorRuntimeParams makeRuntimeParams(DruidCluster cluster, DataSegment... segments)
  {
    return DruidCoordinatorRuntimeParams
        .builder()
        .withDruidCluster(cluster)
        .withBalancerStrategy(balancerStrategy)
        .withUsedSegments(segments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withSmartSegmentLoading(false)
                                    .withUseRoundRobinSegmentAssignment(false)
                                    .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();
  }

  private DruidServer createDruidServer(String tier)
  {
    final String serverName = "hist_" + tier + "_" + serverId.incrementAndGet();
    return new DruidServer(serverName, serverName, null, 10L << 30, null, ServerType.HISTORICAL, tier, 0);
  }

  private ServerHolder createServer(String tier, DataSegment... segments)
  {
    final DruidServer server = createDruidServer(tier);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  /**
   * Creates a server that already serves the given segment. If {@code profile} is non-null, the segment is announced
   * with that partial-load profile attached (matching/stale/full-fallback depending on the test's needs); if
   * {@code profile} is null, the segment is loaded as a regular full-load.
   */
  private ServerHolder createServerWithLoaded(String tier, DataSegment segment, @Nullable PartialLoadProfile profile)
  {
    final DruidServer server = createDruidServer(tier);
    server.addDataSegment(segment, profile);
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  private ServerHolder createDecommissioningServer(String tier)
  {
    return new ServerHolder(createDruidServer(tier).toImmutableDruidServer(), new TestLoadQueuePeon(), true);
  }

  private static DataSegment createSegment()
  {
    return DataSegment
        .builder(SegmentId.of(TestDataSource.WIKI, Intervals.of("2024/2025"), DateTimes.nowUtc().toString(), null))
        .loadSpec(Map.of("type", "local", "path", "/var/druid/segments/foo"))
        .dimensions(Collections.emptyList())
        .metrics(Collections.emptyList())
        .size(0)
        .build();
  }

  private static DataSegment segmentWithProjections(List<String> projections)
  {
    return DataSegment
        .builder(SegmentId.of(TestDataSource.WIKI, Intervals.of("2024/2025"), DateTimes.nowUtc().toString(),
                              new NumberedShardSpec(0, 0)))
        .loadSpec(Map.of("type", "local", "path", "/var/druid/segments/foo"))
        .projections(projections)
        .size(0)
        .build();
  }

  private static PartialLoadProfile profileForRevenue()
  {
    return PartialLoadProfile.forRequest(
        Map.of(
            "type", "partialProjection",
            "delegate", Map.of("type", "local", "path", "/var/druid/segments/foo"),
            "projections", List.of("revenue"),
            "fingerprint", FP_REVENUE
        ),
        FP_REVENUE
    );
  }
}
