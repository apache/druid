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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.CannotMatchBehavior;
import org.apache.druid.server.coordinator.rules.ForeverPartialLoadRule;
import org.apache.druid.server.coordinator.rules.PeriodPartialLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.coordinator.rules.WildcardClusterGroupPartialLoadMatcher;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
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

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;
  private SegmentLoadQueueManager loadQueueManager;

  @Before
  public void setUp()
  {
    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "RunRulesPartialLoadPlacementTest-%d"));
    balancerStrategy = new CostBalancerStrategy(exec);
    loadQueueManager = new SegmentLoadQueueManager(null, null);
  }

  @After
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

    Assert.assertEquals(
        "both core partitions of a fully-unmatched group are empty loaded, not discarded",
        2L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE)
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

    Assert.assertEquals(
        "appended unmatched segment is empty partial loaded",
        1L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE)
    );
    Assert.assertFalse(
        "appended unmatched segment must not be fully downloaded",
        stats.hasStat(Stats.Segments.ASSIGNED)
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
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE));
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

    Assert.assertEquals(
        "all 3 unmatched core partitions are partial loaded so the group stays queryable on demand",
        3L,
        stats.getSegmentStat(Stats.Segments.PARTIAL_ASSIGNED, TIER, DATASOURCE)
    );
    Assert.assertFalse("no partition is fully downloaded", stats.hasStat(Stats.Segments.ASSIGNED));
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
    final RunRules ruleRunner = new RunRules((ds, set) -> set.size(), datasource -> rules);

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .builder()
        .withDruidCluster(cluster)
        .withUsedSegments(segments)
        .withBalancerStrategy(balancerStrategy)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withSmartSegmentLoading(false)
                                    .withUseRoundRobinSegmentAssignment(false)
                                    .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    params = ruleRunner.run(params);
    return params.getCoordinatorStats();
  }

  private static DruidCluster singleTierCluster()
  {
    return DruidCluster.builder().addTier(TIER, historical("hist1", TIER)).build();
  }

  private static ServerHolder historical(String name, String tier)
  {
    final DruidServer server =
        new DruidServer(name, name, null, 10L << 30, null, ServerType.HISTORICAL, tier, 0);
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
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
