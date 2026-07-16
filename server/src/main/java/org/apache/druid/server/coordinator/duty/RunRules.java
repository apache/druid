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
import org.apache.druid.server.coordinator.rules.SegmentActionHandler;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
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
    // Wrap the assigner with a per-shard-group buffering handler. Partial-load matchers that resolve asymmetrically
    // across siblings dispatch their "no positive content for this segment" decision as an empty-fingerprint
    // partial-load. The buffer holds those decisions until we finish iterating the shard group; if any sibling
    // produced a positive match we flush them (preserving broker-side shard-group completeness), otherwise we discard
    // them (avoiding wasted empty loads on groups with no positive content for the matcher anywhere).
    final EmptyDeferringHandler segmentHandler = new EmptyDeferringHandler(segmentAssigner);

    final DateTime now = DateTimes.nowUtc();
    final Object2IntOpenHashMap<String> datasourceToSegmentsWithNoRule = new Object2IntOpenHashMap<>();

    // Streaming shard-group boundary state. SegmentHolder.NEWEST_SEGMENT_FIRST groups segments contiguously by
    // (dataSource, interval, version), so on any change in that triple we flush the buffer for the previous group.
    String currentDs = null;
    Interval currentInterval = null;
    String currentVersion = null;

    for (DataSegment segment : usedSegments) {
      // Do not apply rules on overshadowed segments as they will be
      // marked unused and eventually unloaded from all historicals
      if (overshadowed.contains(segment)) {
        continue;
      }

      // Detect a shard-group boundary and flush deferred empty loads for the previous group.
      if (!segment.getDataSource().equals(currentDs)
          || !segment.getInterval().equals(currentInterval)
          || !segment.getVersion().equals(currentVersion)) {
        segmentHandler.flushAndReset();
        currentDs = segment.getDataSource();
        currentInterval = segment.getInterval();
        currentVersion = segment.getVersion();
      }

      // Find and apply matching rule
      List<Rule> rules = ruleHandler.getRulesWithDefault(segment.getDataSource());
      boolean foundMatchingRule = false;
      for (Rule rule : rules) {
        if (rule.appliesTo(segment, now)) {
          rule.run(segment, segmentHandler);
          foundMatchingRule = true;
          break;
        }
      }

      if (!foundMatchingRule) {
        datasourceToSegmentsWithNoRule.addTo(segment.getDataSource(), 1);
      }
    }

    // Tail flush for the last shard group.
    segmentHandler.flushAndReset();

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
   * Per-shard-group buffering decorator for {@link SegmentActionHandler}. Intercepts partial-load dispatches with
   * the {@link PartialLoadMatcher#EMPTY_LOAD_FINGERPRINT} sentinel and holds them until {@link #flushAndReset} is
   * called by the surrounding shard-group iteration. Positive partial-loads (non-empty fingerprint) pass through
   * immediately and mark the current group as having a positive match.
   *
   * <p>At flush time, buffered empties are dispatched only if a positive match was seen in the same group. This
   * preserves broker-side shard-group completeness for asymmetric matchers while avoiding empty loads when no segment
   * in the group has positive content for the matcher.
   *
   * <p>All other handler methods (full load, broadcast, delete) pass through to the delegate unchanged.
   */
  static final class EmptyDeferringHandler implements SegmentActionHandler
  {
    private final SegmentActionHandler delegate;
    private final List<DeferredEmpty> pendingEmpties = new ArrayList<>();
    private boolean anyPositiveInGroup = false;

    EmptyDeferringHandler(SegmentActionHandler delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public void replicateSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
    {
      delegate.replicateSegment(segment, tierToReplicaCount);
    }

    @Override
    public void replicateSegmentPartially(
        DataSegment segment,
        PartialLoadProfile profile,
        Map<String, Integer> tierToReplicaCount
    )
    {
      if (PartialLoadMatcher.EMPTY_LOAD_FINGERPRINT.equals(profile.fingerprint())) {
        pendingEmpties.add(new DeferredEmpty(segment, profile, tierToReplicaCount));
      } else {
        anyPositiveInGroup = true;
        delegate.replicateSegmentPartially(segment, profile, tierToReplicaCount);
      }
    }

    @Override
    public void broadcastSegment(DataSegment segment)
    {
      delegate.broadcastSegment(segment);
    }

    @Override
    public void deleteSegment(DataSegment segment)
    {
      delegate.deleteSegment(segment);
    }

    void flushAndReset()
    {
      if (anyPositiveInGroup) {
        for (DeferredEmpty d : pendingEmpties) {
          delegate.replicateSegmentPartially(d.segment, d.profile, d.tierToReplicaCount);
        }
      }
      pendingEmpties.clear();
      anyPositiveInGroup = false;
    }

    private record DeferredEmpty(
        DataSegment segment,
        PartialLoadProfile profile,
        Map<String, Integer> tierToReplicaCount
    )
    {
    }
  }
}
