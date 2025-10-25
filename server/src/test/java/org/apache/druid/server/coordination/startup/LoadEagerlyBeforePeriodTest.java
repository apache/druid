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

package org.apache.druid.server.coordination.startup;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestSegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class LoadEagerlyBeforePeriodTest
{
  @Test
  public void testLoadEagerlyForSegments()
  {
    DateTime now = DateTimes.nowUtc();
    LoadEagerlyBeforePeriod strategy = new LoadEagerlyBeforePeriod(Period.days(7));

    final DataSegment withinRange = TestSegmentUtils.makeSegment("foo2", "v1", new Interval(now.minusDays(2), now.minusDays(1)));
    final DataSegment infiniteRange = TestSegmentUtils.makeSegment("foo1", "v1", Intervals.ETERNITY);
    final DataSegment overlappingRange = TestSegmentUtils.makeSegment("foo3", "v1", new Interval(now.minusDays(8), now.minusDays(6)));

    Assert.assertFalse(strategy.shouldLoadLazily(withinRange));
    Assert.assertFalse(strategy.shouldLoadLazily(infiniteRange));
    Assert.assertFalse(strategy.shouldLoadLazily(overlappingRange));
  }

  @Test
  public void testLoadLazilyForSegments()
  {
    LoadEagerlyBeforePeriod strategy = new LoadEagerlyBeforePeriod(Period.days(1));
    DateTime now = DateTimes.nowUtc();
    final DataSegment segmentInFuture = TestSegmentUtils.makeSegment("foo2", "v1", new Interval(now.plusDays(1), now.plusDays(2)));
    final DataSegment segmentTooLate = TestSegmentUtils.makeSegment("foo3", "v1", new Interval(now.minusDays(8), now.minusDays(7)));

    Assert.assertTrue(strategy.shouldLoadLazily(segmentInFuture));
    Assert.assertTrue(strategy.shouldLoadLazily(segmentTooLate));
  }
}
