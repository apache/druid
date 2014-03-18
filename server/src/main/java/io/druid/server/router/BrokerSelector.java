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

package io.druid.server.router;

import com.google.inject.Inject;
import io.druid.query.Query;
import io.druid.server.coordinator.rules.LoadRule;
import io.druid.server.coordinator.rules.Rule;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class BrokerSelector<T>
{
  private final CoordinatorRuleManager ruleManager;
  private final TierConfig tierConfig;

  @Inject
  public BrokerSelector(CoordinatorRuleManager ruleManager, TierConfig tierConfig)
  {
    this.ruleManager = ruleManager;
    this.tierConfig = tierConfig;
  }

  public String select(final Query<T> query)
  {
    if (!ruleManager.isStarted()) {
      return null;
    }

    List<Rule> rules = ruleManager.getRulesWithDefault((query.getDataSource()).getName());

    // find the rule that can apply to the entire set of intervals
    DateTime now = new DateTime();
    int lastRulePosition = -1;
    LoadRule baseRule = null;

    for (Interval interval : query.getIntervals()) {
      int currRulePosition = 0;
      for (Rule rule : rules) {
        if (rule instanceof LoadRule && currRulePosition > lastRulePosition && rule.appliesTo(interval, now)) {
          lastRulePosition = currRulePosition;
          baseRule = (LoadRule) rule;
          break;
        }
        currRulePosition++;
      }
    }

    if (baseRule == null) {
      return null;
    }

    // in the baseRule, find the broker of highest priority
    String brokerName = null;
    for (Map.Entry<String, String> entry : tierConfig.getTierToBrokerMap().entrySet()) {
      if (baseRule.getTieredReplicants().containsKey(entry.getKey())) {
        brokerName = entry.getValue();
        break;
      }
    }

    return brokerName;
  }
}
