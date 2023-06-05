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

import com.google.common.collect.Lists;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Duty to run retention rules.
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
  private static final int MAX_MISSING_RULES = 10;

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final DruidCluster cluster = params.getDruidCluster();
    if (cluster.isEmpty()) {
      log.warn("Cluster has no servers. Not running any rules.");
      return params;
    }

    // Get used segments which are overshadowed by other used segments. Those would not need to be loaded and
    // eventually will be unloaded from Historical servers. Segments overshadowed by *served* used segments are marked
    // as unused in MarkAsUnusedOvershadowedSegments, and then eventually Coordinator sends commands to Historical nodes
    // to unload such segments in UnloadUnusedSegments.
    final Set<DataSegment> overshadowed = params.getDataSourcesSnapshot().getOvershadowedSegments();
    final Set<DataSegment> usedSegments = params.getUsedSegments();
    log.info(
        "Applying retention rules on [%d] used segments, skipping [%d] overshadowed segments.",
        usedSegments.size(), overshadowed.size()
    );

    final StrategicSegmentAssigner segmentAssigner = params.getSegmentAssigner();
    final MetadataRuleManager databaseRuleManager = params.getDatabaseRuleManager();

    int missingRules = 0;
    final DateTime now = DateTimes.nowUtc();
    final List<SegmentId> segmentsWithMissingRules = Lists.newArrayListWithCapacity(MAX_MISSING_RULES);

    // Run through all matched rules for used segments
    for (DataSegment segment : usedSegments) {
      if (overshadowed.contains(segment)) {
        // Skip overshadowed segments
        continue;
      }
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
        if (segmentsWithMissingRules.size() < MAX_MISSING_RULES) {
          segmentsWithMissingRules.add(segment.getId());
        }
        missingRules++;
      }
    }

    if (!segmentsWithMissingRules.isEmpty()) {
      log.makeAlert("Unable to find matching rules!")
         .addData("segmentsWithMissingRulesCount", missingRules)
         .addData("segmentsWithMissingRules", segmentsWithMissingRules)
         .emit();
    }

    return params.buildFromExisting()
                 .withBroadcastDatasources(getBroadcastDatasources(params))
                 .build();
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
