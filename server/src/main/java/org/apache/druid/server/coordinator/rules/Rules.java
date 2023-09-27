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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.List;

public class Rules
{
  public static boolean eligibleForLoad(Interval src, Interval target)
  {
    return src.overlaps(target);
  }

  public static boolean eligibleForLoad(Period period, Interval interval, DateTime referenceTimestamp, boolean includeFuture)
  {
    final Interval currInterval = new Interval(period, referenceTimestamp);
    if (includeFuture) {
      return currInterval.getStartMillis() < interval.getEndMillis();
    } else {
      return eligibleForLoad(currInterval, interval);
    }
  }

  private Rules()
  {
  }

  /**
   * Validates the given list of retention rules for a datasource (or cluster default).
   * A rule is considered valid only if it will be evaluated at some point, i.e. its eligible interval
   * is not fully covered by the eligible interval of any preceding rule in the list.
   * <p>
   * Consider two rules r1 and r2. Assume r1 and r2's eligible intervals at the time when the rules are evaluated are
   * i1 and i2 respectively. r1 and r2 are invalid if:
   *  <ul>
   *    <li> i1 is eternity. i.e., eternity fully covers i2 and any other interval that follows it. Or </li>
   *    <li> i1 fully contains i2 and </li>
   *    <li> r1's eligible interval at i1's start and end fully contain r2's eligible interval at i1's start and end
   *    respectively. This boundary check is used to identify rules that will fire at some point. i.e., period based rules
   *    will return distinct eligible intervals at the boundaries, whereas broadcast and interval based rules will return
   *    fixed intervals regardless of the boundary.  </li>
   *  </ul>
   *  </p>
   * @throws org.apache.druid.error.DruidException with error code "invalidInput" if any of the given rules is not valid.
   */
  public static void validateRules(final List<Rule> rules)
  {
    if (rules == null) {
      return;
    }

    final DateTime now = DateTimes.nowUtc();
    for (int i = 0; i < rules.size(); i++) {
      final Rule currRule = rules.get(i);
      final Interval currInterval = currRule.getEligibleInterval(now);

      for (int j = i + 1; j < rules.size(); j++) {
        final Rule nextRule = rules.get(j);
        final Interval nextInterval = nextRule.getEligibleInterval(now);
        if (currInterval.contains(nextInterval)) {
          if (Intervals.ETERNITY.equals(currInterval) ||
              (currRule.getEligibleInterval(currInterval.getStart())
                       .contains(nextRule.getEligibleInterval(currInterval.getStart()))
               && currRule.getEligibleInterval(currInterval.getEnd())
                          .contains(nextRule.getEligibleInterval(currInterval.getEnd())))) {
            throw InvalidInput.exception(
                "Rule[%s] has an interval that fully contains the interval for rule[%s]."
                + " i.e., interval[%s] hides interval[%s]. Please fix the rules and retry.",
                currRule,
                nextRule,
                currInterval,
                nextInterval
            );
          }
        }
      }
    }
  }
}
