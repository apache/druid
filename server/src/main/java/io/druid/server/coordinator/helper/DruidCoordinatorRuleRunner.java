/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.metadata.MetadataRuleManager;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.SegmentReplicantLookup;
import io.druid.server.coordinator.rules.Rule;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Set;

/**
 */
public class DruidCoordinatorRuleRunner implements DruidCoordinatorHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorRuleRunner.class);
  private static int MAX_MISSING_RULES = 10;

  private final ReplicationThrottler replicatorThrottler;

  private final DruidCoordinator coordinator;
  private final int ruleLazyTicks;
  private int currentTick;

  public DruidCoordinatorRuleRunner(DruidCoordinator coordinator, int ruleLazyTicks)
  {
    this(
        new ReplicationThrottler(
            coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
            coordinator.getDynamicConfigs().getReplicantLifetime()
        ),
        coordinator,
        ruleLazyTicks
    );
  }

  public DruidCoordinatorRuleRunner(ReplicationThrottler replicatorThrottler, DruidCoordinator coordinator) {
    this(replicatorThrottler, coordinator, 1);
  }

  public DruidCoordinatorRuleRunner(
      ReplicationThrottler replicatorThrottler,
      DruidCoordinator coordinator,
      int ruleLazyTicks
  )
  {
    this.replicatorThrottler = replicatorThrottler;
    this.coordinator = coordinator;
    this.ruleLazyTicks = ruleLazyTicks;
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

    for (String tier : cluster.getTierNames()) {
      replicatorThrottler.updateReplicationState(tier);
      replicatorThrottler.updateTerminationState(tier);
    }

    DruidCoordinatorRuntimeParams paramsWithReplicationManager = params.buildFromExisting()
                                                                       .withReplicationManager(replicatorThrottler)
                                                                       .build();

    // Run through all matched rules for available segments
    DateTime now = new DateTime();
    MetadataRuleManager databaseRuleManager = paramsWithReplicationManager.getDatabaseRuleManager();

    final List<String> segmentsWithMissingRules = Lists.newArrayListWithCapacity(MAX_MISSING_RULES);
    int missingRules = 0;
    for (DataSegment segment : getTargetSegments(paramsWithReplicationManager)) {
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
          segmentsWithMissingRules.add(segment.getIdentifier());
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

    return paramsWithReplicationManager.buildFromExisting()
                                       .withCoordinatorStats(stats)
                                       .build();
  }

  protected Set<DataSegment> getTargetSegments(DruidCoordinatorRuntimeParams coordinatorParam)
  {
    if (++currentTick < ruleLazyTicks) {
      final SegmentReplicantLookup replicantLookup = coordinatorParam.getSegmentReplicantLookup();
      return DruidCoordinator.makeOrdered(
          Iterables.filter(
              coordinator.getAvailableDataSegments(), new Predicate<DataSegment>()
              {
                @Override
                public boolean apply(DataSegment input)
                {
                  return replicantLookup.getTotalReplicants(input.getIdentifier()) == 0;
                }
              }
          )
      );
    }
    currentTick = 0;
    return coordinatorParam.getAvailableSegments();
  }
}
