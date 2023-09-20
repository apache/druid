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
   * Validate rules. This method throws an exception if a rule contain an interval that
   * will fully cover the next rules' interval in the list. Rules that will be evaluated at some point
   * are considered to be legitimate.
   * @param rules Datasource rules.
   */
  public static void validateRules(final List<Rule> rules)
  {
    if (rules == null) {
      return;
    }
    final DateTime now = DateTimes.nowUtc();
    for (int i = 0; i < rules.size() - 1; i++) {
      final Rule currRule = rules.get(i);
      final Rule nextRule = rules.get(i + 1);
      final Interval currInterval = currRule.getEligibleInterval(now);
      final Interval nextInterval = nextRule.getEligibleInterval(now);
      if (currInterval.contains(nextInterval)) {
        // If the current rule has an eternity interval, it covers everything following it.
        // Or if the current rule still covers the next rule at the current interval boundaries, then the
        // next rule will never fire at any time, so throw an exception.
        if (Intervals.ETERNITY.equals(currInterval) ||
            (currRule.getEligibleInterval(currInterval.getStart()).contains(nextRule.getEligibleInterval(currInterval.getStart()))
             && currRule.getEligibleInterval(currInterval.getEnd()).contains(nextRule.getEligibleInterval(currInterval.getEnd())))) {
          throw InvalidInput.exception(
              "Rule[%s] has an interval that contains interval for rule[%s]. The interval[%s] also covers interval[%s].",
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
