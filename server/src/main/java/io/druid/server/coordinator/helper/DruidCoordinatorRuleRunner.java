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
import com.google.common.collect.Ordering;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.metadata.MetadataRuleManager;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.rules.Rule;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidCoordinatorRuleRunner implements DruidCoordinatorHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorRuleRunner.class);
  private static int MAX_MISSING_RULES = 10;

  private final ReplicationThrottler replicatorThrottler;

  private final DruidCoordinator coordinator;

  public DruidCoordinatorRuleRunner(DruidCoordinator coordinator)
  {
    this(
        new ReplicationThrottler(
            coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
            coordinator.getDynamicConfigs().getReplicantLifetime()
        ),
        coordinator
    );
  }

  public DruidCoordinatorRuleRunner(ReplicationThrottler replicatorThrottler, DruidCoordinator coordinator)
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

    // find available segments which are not overshadowed by other segments in DB
    // only those would need to be loaded/dropped
    // anything overshadowed by served segments is dropped automatically by DruidCoordinatorCleanupOvershadowed
    Map<String, VersionedIntervalTimeline<String, DataSegment>> timelines = new HashMap<>();
    for (DataSegment segment : params.getAvailableSegments()) {
      VersionedIntervalTimeline<String, DataSegment> timeline = timelines.get(segment.getDataSource());
      if (timeline == null) {
        timeline = new VersionedIntervalTimeline<>(Ordering.natural());
        timelines.put(segment.getDataSource(), timeline);
      }

      timeline.add(
          segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment)
      );
    }

    Set<DataSegment> overshadowed = new HashSet<>();
    for (VersionedIntervalTimeline<String, DataSegment> timeline : timelines.values()) {
      for (TimelineObjectHolder<String, DataSegment> holder : timeline.findOvershadowed()) {
        for (DataSegment dataSegment : holder.getObject().payloads()) {
          overshadowed.add(dataSegment);
        }
      }
    }

    Set<DataSegment> nonOvershadowed = new HashSet<>();
    for (DataSegment dataSegment : params.getAvailableSegments()) {
      if (!overshadowed.contains(dataSegment)) {
        nonOvershadowed.add(dataSegment);
      }
    }

    for (String tier : cluster.getTierNames()) {
      replicatorThrottler.updateReplicationState(tier);
    }

    DruidCoordinatorRuntimeParams paramsWithReplicationManager = params.buildFromExistingWithoutAvailableSegments()
                                                                       .withReplicationManager(replicatorThrottler)
                                                                       .withAvailableSegments(nonOvershadowed)
                                                                       .build();

    // Run through all matched rules for available segments
    DateTime now = DateTimes.nowUtc();
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

    return paramsWithReplicationManager.buildFromExistingWithoutAvailableSegments()
                                       .withCoordinatorStats(stats)
                                       .withAvailableSegments(params.getAvailableSegments())
                                       .build();
  }
}
