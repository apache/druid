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

package io.druid.server.coordinator.rules;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 */
public class RuleMap
{
  private final Map<String, List<Rule>> rules;
  private final List<Rule> defaultRules;

  public RuleMap(Map<String, List<Rule>> rules, List<Rule> defaultRules)
  {
    this.rules = rules;
    this.defaultRules = defaultRules;
  }

  public List<Rule> getRules(String dataSource)
  {
    List<Rule> retVal = Lists.newArrayList();
    if (dataSource != null) {
      retVal.addAll(rules.get(dataSource));
    }
    if (defaultRules != null) {
      retVal.addAll(defaultRules);
    }
    return retVal;
  }
}
