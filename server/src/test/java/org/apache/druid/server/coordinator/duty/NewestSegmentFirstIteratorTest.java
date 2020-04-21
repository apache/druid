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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class NewestSegmentFirstIteratorTest
{
  @Test
  public void testFilterSkipIntervals()
  {
    final Interval totalInterval = Intervals.of("2018-01-01/2019-01-01");
    final List<Interval> expectedSkipIntervals = ImmutableList.of(
        Intervals.of("2018-01-15/2018-03-02"),
        Intervals.of("2018-07-23/2018-10-01"),
        Intervals.of("2018-10-02/2018-12-25"),
        Intervals.of("2018-12-31/2019-01-01")
    );
    final List<Interval> skipIntervals = NewestSegmentFirstIterator.filterSkipIntervals(
        totalInterval,
        Lists.newArrayList(
            Intervals.of("2017-12-01/2018-01-15"),
            Intervals.of("2018-03-02/2018-07-23"),
            Intervals.of("2018-10-01/2018-10-02"),
            Intervals.of("2018-12-25/2018-12-31")
        )
    );

    Assert.assertEquals(expectedSkipIntervals, skipIntervals);
  }

  @Test
  public void testAddSkipIntervalFromLatestAndSort()
  {
    final List<Interval> expectedIntervals = ImmutableList.of(
        Intervals.of("2018-12-24/2018-12-25"),
        Intervals.of("2018-12-29/2019-01-01")
    );
    final List<Interval> fullSkipIntervals = NewestSegmentFirstIterator.sortAndAddSkipIntervalFromLatest(
        DateTimes.of("2019-01-01"),
        new Period(72, 0, 0, 0),
        ImmutableList.of(
            Intervals.of("2018-12-30/2018-12-31"),
            Intervals.of("2018-12-24/2018-12-25")
        )
    );

    Assert.assertEquals(expectedIntervals, fullSkipIntervals);
  }
}
