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
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.List;
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

  private final SegmentDeleteHandler deleteHandler;

  public RunRules(SegmentDeleteHandler deleteHandler)
  {
    this.deleteHandler = deleteHandler;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final DruidCluster cluster = params.getDruidCluster();
    if (cluster.isEmpty()) {
      log.warn("Cluster has no servers. Not running any rules.");
      return params;
    }

    final Set<DataSegment> overshadowed = params.getDataSourcesSnapshot().getOvershadowedSegments();
    final Set<DataSegment> usedSegments = params.getUsedSegments();
    log.info(
        "Applying retention rules on [%,d] used segments, skipping [%,d] overshadowed segments.",
        usedSegments.size(), overshadowed.size()
    );

    final StrategicSegmentAssigner segmentAssigner = params.getSegmentAssigner();
    final MetadataRuleManager databaseRuleManager = params.getDatabaseRuleManager();

    final DateTime now = DateTimes.nowUtc();
    final Object2IntOpenHashMap<String> datasourceToSegmentsWithNoRule = new Object2IntOpenHashMap<>();
    for (DataSegment segment : usedSegments) {
      // Do not apply rules on overshadowed segments as they will be
      // marked unused and eventually unloaded from all historicals
      if (overshadowed.contains(segment)) {
        continue;
      }

      // Find and apply matching rule
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(segment.getDataSource());
      boolean foundMatchingRule = false;
      for (Rule rule : rules) {
        if (rule.appliesTo(segment, now)) {
          rule.run(segment, segmentAssigner);
          foundMatchingRule = true;
          break;
        }
      }

      if (!foundMatchingRule) {
        datasourceToSegmentsWithNoRule.addTo(segment.getDataSource(), 1);
      }
    }

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
      int numUpdatedSegments = deleteHandler.markSegmentsAsUnused(segmentIds);

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
    final Set<String> broadcastDatasources =
        params.getDataSourcesSnapshot().getDataSourcesMap().values().stream()
              .map(ImmutableDruidDataSource::getName)
              .filter(datasource -> isBroadcastDatasource(datasource, params))
              .collect(Collectors.toSet());

    if (!broadcastDatasources.isEmpty()) {
      log.info("Found broadcast datasources [%s] which will not participate in balancing.", broadcastDatasources);
    }

    return broadcastDatasources;
  }

  /**
   * A datasource is considered a broadcast datasource if it has even one
   * Broadcast Rule. Segments of broadcast datasources:
   * <ul>
   *   <li>Do not participate in balancing</li>
   *   <li>Are unloaded if unused, even from realtime servers</li>
   * </ul>
   */
  private boolean isBroadcastDatasource(String datasource, DruidCoordinatorRuntimeParams params)
  {
    return params.getDatabaseRuleManager().getRulesWithDefault(datasource).stream()
                 .anyMatch(rule -> rule instanceof BroadcastDistributionRule);
  }
}
