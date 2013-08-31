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

package io.druid.server.master;

import com.metamx.emitter.EmittingLogger;
import io.druid.client.DataSegment;
import io.druid.db.DatabaseRuleManager;
import io.druid.server.master.rules.Rule;
import org.joda.time.DateTime;

import java.util.List;

/**
 */
public class DruidMasterRuleRunner implements DruidMasterHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidMasterRuleRunner.class);

  private final ReplicationThrottler replicatorThrottler;

  private final DruidMaster master;

  public DruidMasterRuleRunner(DruidMaster master, int replicantLifeTime, int replicantThrottleLimit)
  {
    this.master = master;
    this.replicatorThrottler = new ReplicationThrottler(replicantThrottleLimit, replicantLifeTime);
  }

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    MasterStats stats = new MasterStats();
    DruidCluster cluster = params.getDruidCluster();

    if (cluster.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    for (String tier : cluster.getTierNames()) {
      replicatorThrottler.updateReplicationState(tier);
      replicatorThrottler.updateTerminationState(tier);
    }

    DruidMasterRuntimeParams paramsWithReplicationManager = params.buildFromExisting()
                                                                  .withReplicationManager(replicatorThrottler)
                                                                  .build();

    // Run through all matched rules for available segments
    DateTime now = new DateTime();
    DatabaseRuleManager databaseRuleManager = paramsWithReplicationManager.getDatabaseRuleManager();
    for (DataSegment segment : paramsWithReplicationManager.getAvailableSegments()) {
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(segment.getDataSource());
      boolean foundMatchingRule = false;
      for (Rule rule : rules) {
        if (rule.appliesTo(segment, now)) {
          stats.accumulate(rule.run(master, paramsWithReplicationManager, segment));
          foundMatchingRule = true;
          break;
        }
      }

      if (!foundMatchingRule) {
        log.makeAlert(
            "Unable to find a matching rule for dataSource[%s]",
            segment.getDataSource()
        )
           .addData("segment", segment.getIdentifier())
           .emit();
      }
    }

    return paramsWithReplicationManager.buildFromExisting()
                                       .withMasterStats(stats)
                                       .build();
  }
}
