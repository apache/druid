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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.PartialLoadMatcher;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.coordinator.rules.RuleRunResult;
import org.apache.druid.server.coordinator.rules.SegmentActionHandler;
import org.apache.druid.server.coordinator.rules.ShardGroupFollowup;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Duty to run retention rules for all used non-overshadowed segments.
 * Overshadowed segments are marked unused by {@link MarkOvershadowedSegmentsAsUnused}
 * duty and are eventually unloaded from all servers by {@link UnloadUnusedSegments}.
 * <p>
 * The params returned from {@code run()} must have these fields initialized:
 * <ul>
 *   <li>{@link DruidCoordinatorRuntimeParams#getBroadcastDatasources()}</li>
 * </ul>
 * These fields are used by the downstream coordinator duty, {@link BalanceSegments}.
 */
public class RunRules implements CoordinatorDuty
{
  private static final EmittingLogger log = new EmittingLogger(RunRules.class);

  private final MetadataAction.DeleteSegments deleteHandler;
  private final MetadataAction.GetDatasourceRules ruleHandler;

  public RunRules(
      MetadataAction.DeleteSegments deleteHandler,
      MetadataAction.GetDatasourceRules ruleHandler
  )
  {
    this.deleteHandler = deleteHandler;
    this.ruleHandler = ruleHandler;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final DruidCluster cluster = params.getDruidCluster();
    if (cluster.isEmpty()) {
      log.warn("Cluster has no servers. Not running any rules.");
      return params;
    }

    // Get all used segments sorted by interval. Segments must be sorted to ensure that:
    // a) round-robin assignment distributes newer segments uniformly across servers
    // b) replication throttling has a smaller impact on newer segments
    final Set<DataSegment> usedSegments = params.getUsedSegmentsNewestFirst();
    final Set<DataSegment> overshadowed = params.getDataSourcesSnapshot().getOvershadowedSegments();

    final StrategicSegmentAssigner segmentAssigner = params.getSegmentAssigner();

    final DateTime now = DateTimes.nowUtc();
    final Object2IntOpenHashMap<String> datasourceToSegmentsWithNoRule = new Object2IntOpenHashMap<>();
    final DataSourcesSnapshot snapshot = params.getDataSourcesSnapshot();

    // Streaming shard-group followup state. The iteration order (SegmentHolder.NEWEST_SEGMENT_FIRST) groups segments
    // contiguously by (dataSource, interval, version), so we can flush the previous group's followups the moment any
    // of those three fields changes. This bounds the buffer to ≤ one shard group's worth of followups instead of the
    // entire run's.
    String currentDs = null;
    Interval currentInterval = null;
    String currentVersion = null;
    final List<ShardGroupFollowup> pendingShardGroupFollowups = new ArrayList<>();

    for (DataSegment segment : usedSegments) {
      // Do not apply rules on overshadowed segments as they will be
      // marked unused and eventually unloaded from all historicals
      if (overshadowed.contains(segment)) {
        continue;
      }

      // Detect a shard-group boundary and flush followups for the previous group.
      if (!segment.getDataSource().equals(currentDs)
          || !segment.getInterval().equals(currentInterval)
          || !segment.getVersion().equals(currentVersion)) {
        flushShardGroupFollowups(
            pendingShardGroupFollowups,
            currentDs,
            currentInterval,
            currentVersion,
            snapshot,
            segmentAssigner
        );
        pendingShardGroupFollowups.clear();
        currentDs = segment.getDataSource();
        currentInterval = segment.getInterval();
        currentVersion = segment.getVersion();
      }

      // Find and apply matching rule
      List<Rule> rules = ruleHandler.getRulesWithDefault(segment.getDataSource());
      boolean foundMatchingRule = false;
      for (Rule rule : rules) {
        if (rule.appliesTo(segment, now)) {
          final RuleRunResult result = rule.run(segment, segmentAssigner);
          if (result instanceof ShardGroupFollowup followup) {
            pendingShardGroupFollowups.add(followup);
          }
          foundMatchingRule = true;
          break;
        }
      }

      if (!foundMatchingRule) {
        datasourceToSegmentsWithNoRule.addTo(segment.getDataSource(), 1);
      }
    }

    // Tail flush for the last shard group.
    flushShardGroupFollowups(
        pendingShardGroupFollowups,
        currentDs,
        currentInterval,
        currentVersion,
        snapshot,
        segmentAssigner
    );

    processSegmentDeletes(segmentAssigner, params.getCoordinatorStats());
    alertForSegmentsWithNoRules(datasourceToSegmentsWithNoRule);
    alertForInvalidRules(segmentAssigner);

    return params.buildFromExisting()
                 .withBroadcastDatasources(getBroadcastDatasources(params))
                 .build();
  }

  private void processSegmentDeletes(
      StrategicSegmentAssigner segmentAssigner,
      CoordinatorRunStats runStats
  )
  {
    segmentAssigner.getSegmentsToDelete().forEach((datasource, segmentIds) -> {
      final Stopwatch stopwatch = Stopwatch.createStarted();
      int numUpdatedSegments = deleteHandler.markSegmentsAsUnused(datasource, segmentIds);

      RowKey rowKey = RowKey.of(Dimension.DATASOURCE, datasource);
      runStats.add(Stats.Segments.DELETED, rowKey, numUpdatedSegments);

      log.info(
          "Successfully marked [%d] segments of datasource[%s] as unused in [%d]ms.",
          numUpdatedSegments, datasource, stopwatch.millisElapsed()
      );
    });
  }

  private void alertForSegmentsWithNoRules(Object2IntOpenHashMap<String> datasourceToSegmentsWithNoRule)
  {
    datasourceToSegmentsWithNoRule.object2IntEntrySet().fastForEach(
        entry -> log.noStackTrace().makeAlert(
            "No matching retention rule for [%d] segments in datasource[%s]",
            entry.getIntValue(), entry.getKey()
        ).emit()
    );
  }

  private void alertForInvalidRules(StrategicSegmentAssigner segmentAssigner)
  {
    segmentAssigner.getDatasourceToInvalidLoadTiers().forEach(
        (datasource, invalidTiers) -> log.makeAlert(
            "Load rules for datasource[%s] refer to invalid tiers[%s]."
            + " Update the load rules or add servers for these tiers.",
            datasource, invalidTiers
        ).emit()
    );
  }

  private Set<String> getBroadcastDatasources(DruidCoordinatorRuntimeParams params)
  {
    return params.getDataSourcesSnapshot().getDataSourcesMap().values().stream()
                 .map(ImmutableDruidDataSource::getName)
                 .filter(this::isBroadcastDatasource)
                 .collect(Collectors.toSet());
  }

  /**
   * A datasource is considered a broadcast datasource if it has even one
   * Broadcast Rule. Segments of broadcast datasources:
   * <ul>
   *   <li>Do not participate in balancing</li>
   *   <li>Are unloaded if unused, even from realtime servers</li>
   * </ul>
   */
  private boolean isBroadcastDatasource(String datasource)
  {
    return ruleHandler.getRulesWithDefault(datasource)
                      .stream()
                      .anyMatch(rule -> rule instanceof BroadcastDistributionRule);
  }

  /**
   * Flush {@link ShardGroupFollowup}s for a single {@code (dataSource, interval, version)} shard group: dispatch
   * {@link PartialLoadMatcher#emptyMatch} loads to siblings that did not get a positive match from the same
   * matcher. Keeps the broker's {@code PartitionHolder.isComplete()} happy when a matcher resolves asymmetrically
   * across siblings.
   *
   * <p>Followups within the group are grouped by matcher reference identity. Matchers are stable for the lifetime
   * of the run, and one segment matches at most one rule, so the key disambiguates correctly without requiring
   * matchers to define equals/hashCode.
   */
  static void flushShardGroupFollowups(
      List<ShardGroupFollowup> pendingFollowups,
      @Nullable String dataSource,
      @Nullable Interval interval,
      @Nullable String version,
      DataSourcesSnapshot snapshot,
      SegmentActionHandler segmentAssigner
  )
  {
    if (pendingFollowups.isEmpty()) {
      return;
    }

    final SegmentTimeline timeline = snapshot.getUsedSegmentsTimelinesPerDataSource().get(dataSource);
    if (timeline == null) {
      return;
    }
    final PartitionHolder<DataSegment> holder = timeline.findChunks(interval, version);
    if (holder == null) {
      return;
    }

    // Group by matcher reference identity (matchers don't define equals).
    final IdentityHashMap<PartialLoadMatcher, MatcherFollowup> byMatcher = new IdentityHashMap<>();
    for (ShardGroupFollowup followup : pendingFollowups) {
      byMatcher.computeIfAbsent(followup.matcher(), k -> new MatcherFollowup(followup.tieredReplicants()))
          .matchedSegmentIds.add(followup.matchedSegment().getId());
    }

    for (Map.Entry<PartialLoadMatcher, MatcherFollowup> entry : byMatcher.entrySet()) {
      final PartialLoadMatcher matcher = entry.getKey();
      final MatcherFollowup mf = entry.getValue();
      for (DataSegment sibling : holder.payloads()) {
        // Only the core partition group has an atomic-replace completeness requirement on the broker. Appended
        // siblings (partitionNum >= numCorePartitions) are queried individually and don't need empty-loads.
        if (sibling.getShardSpec().getPartitionNum() >= sibling.getShardSpec().getNumCorePartitions()) {
          continue;
        }
        if (mf.matchedSegmentIds.contains(sibling.getId())) {
          continue;
        }
        final PartialLoadMatcher.MatchResult emptyResult = matcher.emptyMatch(sibling, sibling.getLoadSpec());
        if (emptyResult == null) {
          continue;
        }
        segmentAssigner.replicateSegmentPartially(
            sibling,
            PartialLoadProfile.forRequest(emptyResult.wrappedLoadSpec(), emptyResult.fingerprint()),
            mf.tieredReplicants
        );
      }
    }
  }

  private static final class MatcherFollowup
  {
    private final Map<String, Integer> tieredReplicants;
    private final Set<SegmentId> matchedSegmentIds = new HashSet<>();

    MatcherFollowup(Map<String, Integer> tieredReplicants)
    {
      this.tieredReplicants = tieredReplicants;
    }
  }
}
