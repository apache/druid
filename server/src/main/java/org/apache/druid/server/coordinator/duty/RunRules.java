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
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class RunRules implements CoordinatorDuty
{
  private static final EmittingLogger log = new EmittingLogger(RunRules.class);
  private static final int MAX_MISSING_RULES = 10;

  private final ReplicationThrottler replicatorThrottler;

  private final DruidCoordinator coordinator;

  public RunRules(DruidCoordinator coordinator)
  {
    this(
        new ReplicationThrottler(
            coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
            coordinator.getDynamicConfigs().getReplicantLifetime()
        ),
        coordinator
    );
  }

  public RunRules(ReplicationThrottler replicatorThrottler, DruidCoordinator coordinator)
  {
    this.replicatorThrottler = replicatorThrottler;
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    replicatorThrottler.updateParams(
        coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
        coordinator.getDynamicConfigs().getReplicantLifetime()
    );

    CoordinatorStats stats = new CoordinatorStats();
    DruidCluster cluster = params.getDruidCluster();

    if (cluster.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    // Get used segments which are overshadowed by other used segments. Those would not need to be loaded and
    // eventually will be unloaded from Historical servers. Segments overshadowed by *served* used segments are marked
    // as unused in MarkAsUnusedOvershadowedSegments, and then eventually Coordinator sends commands to Historical nodes
    // to unload such segments in UnloadUnusedSegments.
    Set<SegmentId> overshadowed = params.getDataSourcesSnapshot().getOvershadowedSegments();

    for (String tier : cluster.getTierNames()) {
      replicatorThrottler.updateReplicationState(tier);
    }

    DruidCoordinatorRuntimeParams paramsWithReplicationManager = params
        .buildFromExistingWithoutSegmentsMetadata()
        .withReplicationManager(replicatorThrottler)
        .build();

    // Run through all matched rules for used segments
    DateTime now = DateTimes.nowUtc();
    MetadataRuleManager databaseRuleManager = paramsWithReplicationManager.getDatabaseRuleManager();

    final List<SegmentId> segmentsWithMissingRules = Lists.newArrayListWithCapacity(MAX_MISSING_RULES);
    int missingRules = 0;

    final Set<String> broadcastDatasources = new HashSet<>();
    for (ImmutableDruidDataSource dataSource : params.getDataSourcesSnapshot().getDataSourcesMap().values()) {
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(dataSource.getName());
      for (Rule rule : rules) {
        // A datasource is considered a broadcast datasource if it has any broadcast rules.
        // The set of broadcast datasources is used by BalanceSegments, so it's important that RunRules
        // executes before BalanceSegments.
        if (rule instanceof BroadcastDistributionRule) {
          broadcastDatasources.add(dataSource.getName());
          break;
        }
      }
    }

    for (DataSegment segment : params.getUsedSegments()) {
      if (overshadowed.contains(segment.getId())) {
        // Skipping overshadowed segments
        continue;
      }
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(segment.getDataSource());
      boolean foundMatchingRule = false;
      for (Rule rule : rules) {
        if (rule.appliesTo(segment, now)) {
          stats.accumulate(rule.run(coordinator, paramsWithReplicationManager, segment));
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
                 .withCoordinatorStats(stats)
                 .withBroadcastDatasources(broadcastDatasources)
                 .build();
  }
}
