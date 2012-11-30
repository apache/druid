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

import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.master.rules.RuleMap;

import java.util.Map;

/**
 */
public class SegmentRuleLookup
{
  // Create a mapping between a segment and a rule, a segment is paired with the first rule it matches
  public static SegmentRuleLookup make(
      Iterable<DataSegment> segments,
      RuleMap ruleMap
  )
  {
    Map<String, Rule> lookup = Maps.newHashMap();

    for (DataSegment segment : segments) {
      for (Rule rule : ruleMap.getRules(segment.getDataSource())) {
        if (rule.appliesTo(segment)) {
          lookup.put(segment.getIdentifier(), rule);
          break;
        }
      }

      if (!lookup.containsKey(segment.getIdentifier())) {
        throw new ISE("Unable to find a rule for [%s]!!!", segment.getIdentifier());
      }
    }
    return new SegmentRuleLookup(lookup);
  }

  private final Map<String, Rule> lookup;

  public SegmentRuleLookup()
  {
    this.lookup = Maps.newHashMap();
  }

  public SegmentRuleLookup(
      Map<String, Rule> lookup
  )
  {
    this.lookup = lookup;
  }

  public Rule lookup(String s)
  {
    return lookup.get(s);
  }
}
