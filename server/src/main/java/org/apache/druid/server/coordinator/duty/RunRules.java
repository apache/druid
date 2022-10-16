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
import com.google.common.collect.Sets;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.SegmentLoader;
import org.apache.druid.server.coordinator.SegmentStateManager;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Duty to run retention rules.
 * <p>
 * The params returned from {@code run()} must have these fields initialized:
 * <ul>
 *   <li>{@link DruidCoordinatorRuntimeParams#getBroadcastDatasources()}</li>
 *   <li>{@link DruidCoordinatorRuntimeParams#getReplicationManager()}</li>
 * </ul>
 * These fields are used by the downstream coordinator duty, {@link BalanceSegments}.
 */
public class RunRules implements CoordinatorDuty
{
  private static final EmittingLogger log = new EmittingLogger(RunRules.class);
  private static final int MAX_MISSING_RULES = 10;

  private final SegmentStateManager stateManager;

  public RunRules(SegmentStateManager stateManager)
  {
    this.stateManager = stateManager;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
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

    // Run through all matched rules for used segments
    DateTime now = DateTimes.nowUtc();
    MetadataRuleManager databaseRuleManager = params.getDatabaseRuleManager();

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

    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    final ReplicationThrottler replicationThrottler = createReplicationThrottler(
        reduceLifetimesAndGetBusyTiers(dynamicConfig),
        cluster,
        dynamicConfig
    );
    final SegmentLoader segmentLoader = new SegmentLoader(
        stateManager,
        params.getDruidCluster(),
        params.getSegmentReplicantLookup(),
        replicationThrottler,
        params.getBalancerStrategy()
    );
    for (DataSegment segment : params.getUsedSegments()) {
      if (overshadowed.contains(segment.getId())) {
        // Skipping overshadowed segments
        continue;
      }
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(segment.getDataSource());
      boolean foundMatchingRule = false;
      for (Rule rule : rules) {
        if (rule.appliesTo(segment, now)) {
          rule.run(segment, segmentLoader);
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

    segmentLoader.makeAlerts();
    return params.buildFromExisting()
                 .withCoordinatorStats(segmentLoader.getStats())
                 .withBroadcastDatasources(broadcastDatasources)
                 .withReplicationManager(replicationThrottler)
                 .build();
  }

  /**
   * Reduces the lifetimes of segments currently being replicated in all the tiers.
   * Returns the set of tiers that are currently replicatinng some segments and
   * won't be eligible for assigning more replicas in this run.
   */
  private Set<String> reduceLifetimesAndGetBusyTiers(CoordinatorDynamicConfig dynamicConfig)
  {
    final Set<String> busyTiers = new HashSet<>();
    stateManager.reduceLifetimesOfReplicatingSegments().forEach((tier, replicatingState) -> {
      int numReplicatingSegments = replicatingState.getNumProcessingSegments();
      if (numReplicatingSegments <= 0) {
        return;
      }

      busyTiers.add(tier);
      log.info(
          "Skipping replication on tier [%s] as is still has %d segments in queue with lifetime [%d / %d]",
          tier,
          numReplicatingSegments,
          replicatingState.getLifetime(),
          dynamicConfig.getReplicantLifetime()
      );

      // Create alerts for stuck tiers
      if (replicatingState.getLifetime() <= 0) {
        log.makeAlert("Replication queue for tier [%s] has [%d] segments stuck.", tier, numReplicatingSegments)
           .addData("segments", replicatingState.getCurrentlyProcessingSegmentsAndHosts())
           .emit();
      }
    });

    return busyTiers;
  }

  private ReplicationThrottler createReplicationThrottler(
      Set<String> busyTiers,
      DruidCluster cluster,
      CoordinatorDynamicConfig dynamicConfig
  )
  {
    // Tiers that already have some replication in progress are not eligible for
    // replication in this coordinator run
    final Set<String> tiersEligibleForReplication = Sets.newHashSet(cluster.getTierNames());
    tiersEligibleForReplication.removeAll(busyTiers);

    return new ReplicationThrottler(
        tiersEligibleForReplication,
        dynamicConfig.getReplicationThrottleLimit(),
        dynamicConfig.getReplicantLifetime(),
        dynamicConfig.getMaxNonPrimaryReplicantsToLoad()
    );
  }
}
