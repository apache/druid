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

package org.apache.druid.indexing.common.task;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class TasksTest
{

  @Test
  public void testComputeCondensedIntervals()
  {
    final SortedSet<Interval> inputIntervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    for (int m = 1; m < 13; m++) {
      for (int d = 1; d < 10; d++) {
        inputIntervals.add(getInterval(m, d, m, d + 1));
      }

      for (int d = 12; d < 20; d++) {
        inputIntervals.add(getInterval(m, d, m, d + 1));
      }

      inputIntervals.add(getInterval(m, 22, m, 23));

      for (int d = 25; d < 28; d++) {
        inputIntervals.add(getInterval(m, d, m, d + 1));
      }

      if (m == 1 || m == 3 || m == 5 || m == 7 || m == 8 || m == 10) {
        inputIntervals.add(getInterval(m, 31, m + 1, 1));
      }
    }

    inputIntervals.add(Intervals.of("2017-12-31/2018-01-01"));

    final SortedSet<Interval> condensedIntervals = Tasks.computeCondensedIntervals(inputIntervals);
    final Iterator<Interval> condensedIntervalIterator = condensedIntervals.iterator();
    Assert.assertTrue(condensedIntervalIterator.hasNext());

    Interval condensedInterval = condensedIntervalIterator.next();
    final SortedSet<Interval> checkedIntervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    for (Interval inputInterval : inputIntervals) {
      if (!condensedInterval.contains(inputInterval)) {
        if (condensedIntervalIterator.hasNext()) {
          condensedInterval = condensedIntervalIterator.next();
          Assert.assertTrue(condensedInterval.contains(inputInterval));
        }
      }
      checkedIntervals.add(inputInterval);
    }

    Assert.assertFalse(condensedIntervalIterator.hasNext());
    Assert.assertEquals(inputIntervals, checkedIntervals);
  }

  private static Interval getInterval(int startMonth, int startDay, int endMonth, int endDay)
  {
    return Intervals.of(
        StringUtils.format(
            "2017-%02d-%02d/2017-%02d-%02d",
            startMonth,
            startDay,
            endMonth,
            endDay
        )
    );
  }
}
