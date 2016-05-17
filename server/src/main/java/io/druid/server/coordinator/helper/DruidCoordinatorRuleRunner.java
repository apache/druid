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

import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.metadata.MetadataRuleManager;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.SegmentProcessingThrottler;
import io.druid.server.coordinator.rules.Rule;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.List;

/**
 */
public class DruidCoordinatorRuleRunner implements DruidCoordinatorHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorRuleRunner.class);
  private static int MAX_MISSING_RULES = 10;

  private final SegmentProcessingThrottler replicantCreationThrottler;
  private final SegmentProcessingThrottler replicantTerminationThrottler;
  private final SegmentProcessingThrottler segmentLoadingThrottler;

  private final DruidCoordinator coordinator;

  public DruidCoordinatorRuleRunner(DruidCoordinator coordinator)
  {
    this(
        new SegmentProcessingThrottler(
            coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
            coordinator.getDynamicConfigs().getReplicantLifetime(),
            "replicantCreation"
        ),
        new SegmentProcessingThrottler(
            coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
            coordinator.getDynamicConfigs().getReplicantLifetime(),
            "replicantTermination"
        ),
        new SegmentProcessingThrottler(
            coordinator.getDynamicConfigs().getLoadingThrottleLimit(),
            coordinator.getDynamicConfigs().getReplicantLifetime(),
            "loading"
        ),
        coordinator
    );
  }

  public DruidCoordinatorRuleRunner(SegmentProcessingThrottler replicantCreationThrottler,
                                    SegmentProcessingThrottler replicantTerminationThrottler,
                                    SegmentProcessingThrottler segmentLoadingThrottler,
       DruidCoordinator coordinator)
  {
    this.replicantCreationThrottler = replicantCreationThrottler;
    this.replicantTerminationThrottler = replicantTerminationThrottler;
    this.segmentLoadingThrottler = segmentLoadingThrottler;
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    replicantCreationThrottler.updateParams(
        coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
        coordinator.getDynamicConfigs().getReplicantLifetime()
    );

    replicantTerminationThrottler.updateParams(
        coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
        coordinator.getDynamicConfigs().getReplicantLifetime()
    );

    segmentLoadingThrottler.updateParams(
        coordinator.getDynamicConfigs().getLoadingThrottleLimit(),
        coordinator.getDynamicConfigs().getReplicantLifetime()
    );

    CoordinatorStats stats = new CoordinatorStats();
    DruidCluster cluster = params.getDruidCluster();

    if (cluster.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    for (String tier : cluster.getTierNames()) {
      replicantCreationThrottler.updateState(tier);
      replicantTerminationThrottler.updateState(tier);
      segmentLoadingThrottler.updateState(tier);
    }

    DruidCoordinatorRuntimeParams paramsWithReplicationManager = params.buildFromExisting()
                                                                       .withReplicantCreationThrottler(replicantCreationThrottler)
                                                                       .withreplicantTerminationThrottler(replicantTerminationThrottler)
                                                                       .withSegmentLoadingThrottler(segmentLoadingThrottler)
                                                                       .build();

    // Run through all matched rules for available segments
    DateTime now = new DateTime();
    MetadataRuleManager databaseRuleManager = paramsWithReplicationManager.getDatabaseRuleManager();

    final List<String> segmentsWithMissingRules = Lists.newArrayListWithCapacity(MAX_MISSING_RULES);
    int missingRules = 0;
    for (DataSegment segment : paramsWithReplicationManager.getAvailableSegments()) {
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
}
