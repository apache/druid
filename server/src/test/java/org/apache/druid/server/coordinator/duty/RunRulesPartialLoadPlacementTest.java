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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.metadata.MetadataRuleManagerConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.SegmentHolder;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.CannotMatchBehavior;
import org.apache.druid.server.coordinator.rules.ForeverPartialLoadRule;
import org.apache.druid.server.coordinator.rules.PeriodPartialLoadRule;
import org.apache.druid.server.coordinator.rules.RetentionRulesSnapshot;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.coordinator.rules.WildcardClusterGroupPartialLoadMatcher;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Placement-level behavior of cluster-group partial-load rules driven through the full {@link RunRules} duty. These
 * assert what actually lands on a tier: a compatible-but-unmatched cluster-group segment is announced (empty loaded)
 * on the claiming rule's tier rather than dropped from every tier or fully downloaded.
 */
public class RunRulesPartialLoadPlacementTest
{
  private static final String DATASOURCE = "ds";
  private static final String TIER = "tier1";
  private static final Interval CHUNK = Intervals.of("2026-01-01/2026-01-02");

  /**
   * Announced profile for a replica loaded under some earlier rule. Its fingerprint matches no rule used here, so
   * the reconciler classifies any replica carrying it as stale.
   */
  private static final PartialLoadProfile STALE_PROFILE =
      PartialLoadProfile.forLoaded(Map.of("type", "partialClusterGroup"), "v1:previous-rule", 1L);

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;
  private SegmentLoadQueueManager loadQueueManager;

  @BeforeEach
  public void setUp()
  {
    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "RunRulesPartialLoadPlacementTest-%d"));
    balancerStrategy = new CostBalancerStrategy(exec);
    loadQueueManager = new SegmentLoadQueueManager(null, null);
  }

  @AfterEach
  public void tearDown()
  {
    exec.shutdown();
  }

  /**
   * A shard group in which every core partition is compatible-but-unmatched must be empty loaded so it stays
   * announced and queryable, rather than being dropped from every tier and invisible to the Broker(s)
   */
  @Test
  public void fullyColdCoreGroup_isWeakLoadedNotDiscarded()
  {
    // Two core partitions of one shard group; neither tuple matches the rule's include pattern.
    final DataSegment core0 = clusteredSegment(new NumberedShardSpec(0, 2), "acme");
    final DataSegment core1 = clusteredSegment(new NumberedShardSpec(1, 2), "globex");

    final CoordinatorRunStats stats = runRules(matchNobodyForeverRule(), core0, core1);

    Assertions.assertEquals(
        2L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE),
        "both core partitions of a fully-unmatched group are empty loaded, not discarded"
    );
  }

  /**
   * An appended (non-core) partition that is compatible-but-unmatched must be empty loaded, not
   * fully downloaded via the cannot-match fallback.
   */
  @Test
  public void appendedColdSegment_isWeakLoadedNotFullLoaded()
  {
    final DataSegment appended = clusteredSegment(new NumberedShardSpec(2, 2), "acme");

    final CoordinatorRunStats stats = runRules(matchNobodyForeverRule(), appended);

    Assertions.assertEquals(
        1L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE),
        "appended unmatched segment is empty partial loaded"
    );
    Assertions.assertFalse(
        stats.hasStat(Stats.Segments.ASSIGNED),
        "appended unmatched segment must not be fully downloaded"
    );
  }

  /**
   * When a group has at least one positively-matched sibling, its unmatched core siblings are empty loaded on the
   * rule's own tier to ensure the core partition set is complete.
   */
  @Test
  public void partiallyHotGroup_coldSiblingWeakLoadedOnOwnTier()
  {
    final DataSegment matched = clusteredSegment(new NumberedShardSpec(0, 2), "acme");
    final DataSegment unmatched = clusteredSegment(new NumberedShardSpec(1, 2), "globex");

    final ForeverPartialLoadRule rule = new ForeverPartialLoadRule(
        ImmutableMap.of(TIER, 1),
        null,
        new WildcardClusterGroupPartialLoadMatcher(List.of(Map.of("tenant", "acme")), null),
        CannotMatchBehavior.FULL_LOAD
    );

    final CoordinatorRunStats stats = runRules(rule, matched, unmatched);

    // Both the positive match and the empty match are loaded
    Assertions.assertEquals(2L, stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE));
  }

  /**
   * A realistic core partition set of 3 (tenant, region) tuples, governed by an in-window P7D period rule whose
   * include pattern matches a tenant none of them carry. Every partition resolves to the empty load, and even with no
   * positive sibling, the whole group must still be empty loaded.
   */
  @Test
  public void fullyColdThreePartitionCoreSet_underInWindowPeriodRule_isWeakLoaded()
  {
    final Interval recent = recentChunk();
    final DataSegment p0 = tenantRegionSegment(recent, new NumberedShardSpec(0, 3), "acme", "us-east-1");
    final DataSegment p1 = tenantRegionSegment(recent, new NumberedShardSpec(1, 3), "acme", "us-west-2");
    final DataSegment p2 = tenantRegionSegment(recent, new NumberedShardSpec(2, 3), "globex", "us-east-1");

    final PeriodPartialLoadRule rule = new PeriodPartialLoadRule(
        Period.days(7),
        null,
        ImmutableMap.of(TIER, 1),
        null,
        new WildcardClusterGroupPartialLoadMatcher(List.of(Map.of("tenant", "biz", "region", "*")), null),
        CannotMatchBehavior.FALL_THROUGH
    );

    final CoordinatorRunStats stats = runRules(rule, p0, p1, p2);

    Assertions.assertEquals(
        3L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE),
        "all 3 unmatched core partitions are partial loaded so the group stays queryable on demand"
    );
    Assertions.assertFalse(stats.hasStat(Stats.Segments.ASSIGNED), "no partition is fully downloaded");
  }

  /**
   * Fresh partial loads must be distributed across the historicals of a tier. With three empty historicals and one
   * required replica, each historical should end up holding roughly a third of the assignments.
   */
  @Test
  public void test_freshPartialLoads_areSpreadAcrossHistoricals()
  {
    final List<DataSegment> segments = dailySegments(12, "acme");

    final Map<String, TestLoadQueuePeon> peons = new LinkedHashMap<>();
    final DruidCluster cluster = tierOf(peons, "hist1", "hist2", "hist3");

    runRules(cluster, matchTenantForeverRule("acme"), segments.toArray(new DataSegment[0]));

    assertAssignmentsSpreadEvenly(peons, segments.size());
  }

  /**
   * A rule change that invalidates every loaded replica must spread the replacement loads across the tier. Each
   * historical starts out holding a third of the replicas under a fingerprint the new rule does not match; the
   * reloads that reconcile them should again land roughly a third on each historical.
   */
  @Test
  public void test_ruleChangeInvalidatingEveryReplica_spreadsReloadsAcrossHistoricals()
  {
    final List<DataSegment> segments = dailySegments(12, "acme");

    // Deal the segments evenly across the three historicals under a fingerprint the new rule will not match.
    final Map<String, List<DataSegment>> preloaded = new LinkedHashMap<>();
    final List<String> names = List.of("hist1", "hist2", "hist3");
    names.forEach(name -> preloaded.put(name, new ArrayList<>()));
    for (int i = 0; i < segments.size(); i++) {
      preloaded.get(names.get(i % names.size())).add(segments.get(i));
    }

    final Map<String, TestLoadQueuePeon> peons = new LinkedHashMap<>();
    final DruidCluster cluster = tierOfPreloadedHistoricals(peons, preloaded);

    runRules(cluster, matchTenantForeverRule("acme"), segments.toArray(new DataSegment[0]));

    assertAssignmentsSpreadEvenly(peons, segments.size());
  }

  /**
   * A deficit larger than the number of historicals that can take a fresh load must still queue each historical at
   * most once. The historical whose stale in-flight load is cancelled becomes a fresh-load candidate, and the
   * stale-loaded historical takes an additive reload; a skip stat here means one of them was offered twice.
   */
  @Test
  public void test_ruleChangeWithStaleInFlightAndStaleLoaded_queuesEachHistoricalOnce()
  {
    final DataSegment segment = dailySegments(1, "acme").getFirst();

    // hist1 is mid-load under a fingerprint the new rule does not match, so cancelling frees it for a fresh load.
    final TestLoadQueuePeon inFlightPeon = new TestLoadQueuePeon();
    inFlightPeon.addInFlightHolder(
        new SegmentHolder(segment, SegmentAction.LOAD, STALE_PROFILE, Duration.standardSeconds(10), null)
    );
    final ServerHolder hist1 = new ServerHolder(server("hist1").toImmutableDruidServer(), inFlightPeon);

    // hist2 already serves the segment under that same stale fingerprint, so it can only reload additively.
    final DruidServer staleServer = server("hist2");
    staleServer.addDataSegment(segment, STALE_PROFILE);
    final TestLoadQueuePeon stalePeon = new TestLoadQueuePeon();
    final ServerHolder hist2 = new ServerHolder(staleServer.toImmutableDruidServer(), stalePeon);

    final DruidCluster cluster = DruidCluster.builder().addTier(TIER, hist1, hist2).build();

    final CoordinatorRunStats stats = runRules(cluster, matchTenantForeverRule("acme", 2), segment);

    Assertions.assertEquals(
        1L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_STALE_CANCELLED, TIER, DATASOURCE),
        "the stale in-flight load is cancelled"
    );
    Assertions.assertEquals(
        2L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE),
        "both required replicas are queued"
    );
    Assertions.assertFalse(
        stats.hasStat(Stats.Segments.ASSIGN_SKIPPED),
        "each historical is offered once, so no assignment is skipped"
    );
    Assertions.assertNotEquals(
        STALE_PROFILE,
        inFlightPeon.getProfileFor(segment),
        "the cancelled historical is reloaded under the rule's fingerprint"
    );
    Assertions.assertNotNull(
        stalePeon.getProfileFor(segment),
        "the stale-loaded historical is queued an additive reload"
    );
  }

  /**
   * Asserts that every segment was assigned somewhere and that no historical carries more than one assignment more
   * than any other. The failure message reports the per-historical counts so an uneven split is readable directly.
   */
  private static void assertAssignmentsSpreadEvenly(Map<String, TestLoadQueuePeon> peons, int expectedTotal)
  {
    final Map<String, Integer> counts = new LinkedHashMap<>();
    peons.forEach((name, peon) -> counts.put(name, peon.getSegmentsToLoad().size()));

    final int total = counts.values().stream().mapToInt(Integer::intValue).sum();
    Assertions.assertEquals(expectedTotal, total, "every segment is assigned somewhere, but counts were " + counts);

    final int max = counts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    final int min = counts.values().stream().mapToInt(Integer::intValue).min().orElse(0);
    Assertions.assertTrue(
        max - min <= 1,
        "assignments are spread across the tier's historicals, but counts were " + counts
    );
  }

  private ForeverPartialLoadRule matchNobodyForeverRule()
  {
    // Include pattern resolves against the "tenant" clustering column (compatible) but matches none of the segments'
    // tuples, so every segment resolves to the empty load. onCannotMatch is irrelevant for a compatible matcher.
    return new ForeverPartialLoadRule(
        ImmutableMap.of(TIER, 1),
        null,
        new WildcardClusterGroupPartialLoadMatcher(List.of(Map.of("tenant", "nobody")), null),
        CannotMatchBehavior.FULL_LOAD
    );
  }

  private CoordinatorRunStats runRules(Rule rule, DataSegment... segments)
  {
    return runRules(singleTierCluster(), rule, segments);
  }

  private CoordinatorRunStats runRules(DruidCluster cluster, Rule rule, DataSegment... segments)
  {
    final List<Rule> rules = Collections.singletonList(rule);
    final RunRules ruleRunner = new RunRules((ds, set) -> set.size());

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .builder()
        .withDruidCluster(cluster)
        .withRetentionRulesSnapshot(
            new RetentionRulesSnapshot(
                Map.of(MetadataRuleManagerConfig.DEFAULT_RULE_NAME, rules),
                MetadataRuleManagerConfig.DEFAULT_RULE_NAME
            )
        )
        .withUsedSegments(segments)
        .withBalancerStrategy(balancerStrategy)
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    params = ruleRunner.run(params);
    return params.getCoordinatorStats();
  }

  private static ForeverPartialLoadRule matchTenantForeverRule(String tenant)
  {
    return matchTenantForeverRule(tenant, 1);
  }

  private static ForeverPartialLoadRule matchTenantForeverRule(String tenant, int replicas)
  {
    return new ForeverPartialLoadRule(
        ImmutableMap.of(TIER, replicas),
        null,
        new WildcardClusterGroupPartialLoadMatcher(List.of(Map.of("tenant", tenant)), null),
        CannotMatchBehavior.FULL_LOAD
    );
  }

  private static DruidCluster singleTierCluster()
  {
    return DruidCluster.builder().addTier(TIER, historical("hist1", TIER)).build();
  }

  /**
   * Builds a tier of empty historicals, exposing each one's peon by name so a test can read back what was assigned
   * to it.
   */
  private static DruidCluster tierOf(Map<String, TestLoadQueuePeon> peonsOut, String... names)
  {
    final DruidCluster.Builder cluster = DruidCluster.builder();
    for (String name : names) {
      final TestLoadQueuePeon peon = new TestLoadQueuePeon();
      peonsOut.put(name, peon);
      cluster.add(new ServerHolder(server(name).toImmutableDruidServer(), peon));
    }
    return cluster.build();
  }

  /**
   * Builds a tier whose historicals already serve the given segments under {@link #STALE_PROFILE}, so a rule
   * resolving to any other fingerprint sees every replica as stale.
   */
  private static DruidCluster tierOfPreloadedHistoricals(
      Map<String, TestLoadQueuePeon> peonsOut,
      Map<String, List<DataSegment>> nameToLoadedSegments
  )
  {
    final DruidCluster.Builder cluster = DruidCluster.builder();
    nameToLoadedSegments.forEach((name, loaded) -> {
      final DruidServer server = server(name);
      loaded.forEach(segment -> server.addDataSegment(segment, STALE_PROFILE));

      final TestLoadQueuePeon peon = new TestLoadQueuePeon();
      peonsOut.put(name, peon);
      cluster.add(new ServerHolder(server.toImmutableDruidServer(), peon));
    });
    return cluster.build();
  }

  private static ServerHolder historical(String name, String tier)
  {
    final DruidServer server =
        new DruidServer(name, name, null, 10L << 30, null, ServerType.HISTORICAL, tier, 0);
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  private static DruidServer server(String name)
  {
    return new DruidServer(name, name, null, 10L << 30, null, ServerType.HISTORICAL, TIER, 0);
  }

  /** One single-partition segment per day, all carrying the same tenant tuple. */
  private static List<DataSegment> dailySegments(int count, String tenant)
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        RowSignature.builder().add("tenant", ColumnType.STRING).build(),
        List.of(Collections.singletonList(tenant))
    );

    final List<DataSegment> segments = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final Interval day = new Interval(CHUNK.getStart().plusDays(i), CHUNK.getStart().plusDays(i + 1));
      segments.add(segment(day, new NumberedShardSpec(0, 1), groups));
    }
    return segments;
  }

  private static DataSegment clusteredSegment(NumberedShardSpec shardSpec, String tenant)
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        RowSignature.builder().add("tenant", ColumnType.STRING).build(),
        List.of(Collections.singletonList(tenant))
    );
    return segment(CHUNK, shardSpec, groups);
  }

  private static DataSegment tenantRegionSegment(
      Interval interval,
      NumberedShardSpec shardSpec,
      String tenant,
      String region
  )
  {
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        RowSignature.builder().add("tenant", ColumnType.STRING).add("region", ColumnType.STRING).build(),
        List.of(List.of(tenant, region))
    );
    return segment(interval, shardSpec, groups);
  }

  private static DataSegment segment(
      Interval interval,
      NumberedShardSpec shardSpec,
      ClusterGroupTuples groups
  )
  {
    return DataSegment
        .builder(SegmentId.of(DATASOURCE, interval, "v", shardSpec))
        .shardSpec(shardSpec)
        .loadSpec(Map.of("type", "local", "path", "/seg"))
        .size(0)
        .clusterGroups(groups)
        .build();
  }

  /** A one-day chunk ending at "now" so an in-window P7D period rule applies to it. */
  private static Interval recentChunk()
  {
    final DateTime end = DateTimes.nowUtc();
    return new Interval(end.minusDays(1), end);
  }
}
