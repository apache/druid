/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.master;

import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.master.rules.RuleMap;
import com.metamx.emitter.EmittingLogger;

import java.util.List;

/**
 */
public class DruidMasterRuleRunner implements DruidMasterHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidMasterRuleRunner.class);

  private final DruidMaster master;

  public DruidMasterRuleRunner(DruidMaster master)
  {
    this.master = master;
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

    // Run through all matched rules for available segments
    DatabaseRuleManager databaseRuleManager = params.getDatabaseRuleManager();
    for (DataSegment segment : params.getAvailableSegments()) {
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(segment.getDataSource());

      boolean foundMatchingRule = false;
      for (Rule rule : rules) {
        if (rule.appliesTo(segment)) {
          stats.accumulate(rule.run(master, params, segment));
          foundMatchingRule = true;
          break;
        }
      }

      if (!foundMatchingRule) {
        log.makeAlert("Unable to find a matching rule for segment[%s]", segment.getIdentifier()).emit();
      }
    }

    return params.buildFromExisting()
                 .withMasterStats(stats)
                 .build();
  }
}
