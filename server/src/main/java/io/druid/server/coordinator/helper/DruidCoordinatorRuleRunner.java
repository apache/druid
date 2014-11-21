/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.db.DatabaseRuleManager;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ReplicationThrottler;
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

    for (String tier : cluster.getTierNames()) {
      replicatorThrottler.updateReplicationState(tier);
      replicatorThrottler.updateTerminationState(tier);
    }

    DruidCoordinatorRuntimeParams paramsWithReplicationManager = params.buildFromExisting()
                                                                       .withReplicationManager(replicatorThrottler)
                                                                       .build();

    // Run through all matched rules for available segments
    DateTime now = new DateTime();
    DatabaseRuleManager databaseRuleManager = paramsWithReplicationManager.getDatabaseRuleManager();

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
