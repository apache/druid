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
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.TestDruidCoordinatorConfig;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

/**
 */
public class KillUnusedSegmentsTest
{

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Test invalid configuration. A Null durationToRetain should trigger an
   * exception since we require operators to explicitly configure this value
   * if they enable the segment killing facility.
   */
  @Test
  public void testInvalidDurationToRetain()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "druid.coordinator.kill.durationToRetain must be non-null if ignoreRetainDuration is false"
    );
    new KillUnusedSegments(
        null,
        null,
        new TestDruidCoordinatorConfig(
            null,
            null,
            Duration.parse("PT76400S"),
            null,
            new Duration(1),
            Duration.parse("PT86400S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1000,
            Duration.ZERO,
            false
        )
    );
  }

  /**
   * Test that retainDuration is properly set based on the value available in the
   * Coordinator config. Positive and Negative durations should work as well as
   * null, if and only if ignoreDurationToRetain is true.
   */
  @Test
  public void testRetainDurationValues()
  {
    // Positive duration to retain
    KillUnusedSegments unusedSegmentsKiller = new KillUnusedSegments(
        null,
        null,
        new TestDruidCoordinatorConfig(
            null,
            null,
            Duration.parse("PT76400S"),
            null,
            new Duration(1),
            Duration.parse("PT86400S"),
            Duration.parse("PT86400S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1000,
            Duration.ZERO,
            false
        )
    );
    Assert.assertEquals((Long) Duration.parse("PT86400S").getMillis(), unusedSegmentsKiller.getRetainDuration());

    // Negative duration to retain
    unusedSegmentsKiller = new KillUnusedSegments(
        null,
        null,
        new TestDruidCoordinatorConfig(
            null,
            null,
            Duration.parse("PT76400S"),
            null,
            new Duration(1),
            Duration.parse("PT86400S"),
            Duration.parse("PT-86400S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1000,
            Duration.ZERO,
            false
        )
    );
    Assert.assertEquals((Long) Duration.parse("PT-86400S").getMillis(), unusedSegmentsKiller.getRetainDuration());

    // Null duration to retain is valid if coordinatorKillIgnoreDurationToRetain is true
    unusedSegmentsKiller = new KillUnusedSegments(
        null,
        null,
        new TestDruidCoordinatorConfig(
            null,
            null,
            Duration.parse("PT76400S"),
            null,
            new Duration(1),
            Duration.parse("PT86400S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1000,
            Duration.ZERO,
            true
        )
    );
    Assert.assertEquals((Long) Duration.parse("PT0S").getMillis(), unusedSegmentsKiller.getRetainDuration());
  }

  /**
   * Test that the end time upper limit is properly computated for both positive and
   * negative durations. Also ensure that if durationToRetain is to be ignored, that
   * the upper limit is {@link DateTime} max time.
   */
  @Test
  public void testGetEndTimeUpperLimit()
  {
    // If ignoreDurationToRetain is true, ignore the value configured for durationToRetain and return 9999-12-31T23:59
    KillUnusedSegments unusedSegmentsKiller = new KillUnusedSegments(
        null,
        null,
        new TestDruidCoordinatorConfig(
            null,
            null,
            Duration.parse("PT76400S"),
            null,
            new Duration(1),
            Duration.parse("PT86400S"),
            Duration.parse("PT86400S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1000,
            Duration.ZERO,
            true
        )
    );
    Assert.assertEquals(
        DateTimes.of(9999, 12, 31, 23, 59),
        unusedSegmentsKiller.getEndTimeUpperLimit()
    );

    // Testing a negative durationToRetain period returns proper date in future
    unusedSegmentsKiller = new KillUnusedSegments(
        null,
        null,
        new TestDruidCoordinatorConfig(
            null,
            null,
            Duration.parse("PT76400S"),
            null,
            new Duration(1),
            Duration.parse("PT86400S"),
            Duration.parse("PT-86400S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1000,
            Duration.ZERO,
            false
        )
    );

    DateTime expectedTime = DateTimes.nowUtc().minus(Duration.parse("PT-86400S").getMillis());
    Assert.assertEquals(expectedTime, unusedSegmentsKiller.getEndTimeUpperLimit());

    // Testing a positive durationToRetain period returns expected value in the past
    unusedSegmentsKiller = new KillUnusedSegments(
        null,
        null,
        new TestDruidCoordinatorConfig(
            null,
            null,
            Duration.parse("PT76400S"),
            null,
            new Duration(1),
            Duration.parse("PT86400S"),
            Duration.parse("PT86400S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1000,
            Duration.ZERO,
            false
        )
    );
    expectedTime = DateTimes.nowUtc().minus(Duration.parse("PT86400S").getMillis());
    Assert.assertEquals(expectedTime, unusedSegmentsKiller.getEndTimeUpperLimit());
  }

  @Test
  public void testFindIntervalForKill()
  {
    testFindIntervalForKill(null, null);
    testFindIntervalForKill(ImmutableList.of(), null);

    testFindIntervalForKill(ImmutableList.of(Intervals.of("2014/2015")), Intervals.of("2014/2015"));

    testFindIntervalForKill(
        ImmutableList.of(Intervals.of("2014/2015"), Intervals.of("2016/2017")),
        Intervals.of("2014/2017")
    );

    testFindIntervalForKill(
        ImmutableList.of(Intervals.of("2014/2015"), Intervals.of("2015/2016")),
        Intervals.of("2014/2016")
    );

    testFindIntervalForKill(
        ImmutableList.of(Intervals.of("2015/2016"), Intervals.of("2014/2015")),
        Intervals.of("2014/2016")
    );

    testFindIntervalForKill(
        ImmutableList.of(Intervals.of("2015/2017"), Intervals.of("2014/2016")),
        Intervals.of("2014/2017")
    );

    testFindIntervalForKill(
        ImmutableList.of(
            Intervals.of("2015/2019"),
            Intervals.of("2014/2016"),
            Intervals.of("2018/2020")
        ),
        Intervals.of("2014/2020")
    );

    testFindIntervalForKill(
        ImmutableList.of(
            Intervals.of("2015/2019"),
            Intervals.of("2014/2016"),
            Intervals.of("2018/2020"),
            Intervals.of("2021/2022")
        ),
        Intervals.of("2014/2022")
    );
  }

  private void testFindIntervalForKill(List<Interval> segmentIntervals, Interval expected)
  {
    SegmentsMetadataManager segmentsMetadataManager = EasyMock.createMock(SegmentsMetadataManager.class);
    EasyMock.expect(
        segmentsMetadataManager.getUnusedSegmentIntervals(
            EasyMock.anyString(),
            EasyMock.anyObject(DateTime.class),
            EasyMock.anyInt()
        )
    ).andReturn(segmentIntervals);
    EasyMock.replay(segmentsMetadataManager);
    IndexingServiceClient indexingServiceClient = EasyMock.createMock(IndexingServiceClient.class);

    KillUnusedSegments unusedSegmentsKiller = new KillUnusedSegments(
        segmentsMetadataManager,
        indexingServiceClient,
        new TestDruidCoordinatorConfig(
            null,
            null,
            Duration.parse("PT76400S"),
            null,
            new Duration(1),
            Duration.parse("PT86400S"),
            Duration.parse("PT86400S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1000,
            Duration.ZERO,
            false
        )
    );

    Assert.assertEquals(
        expected,
        unusedSegmentsKiller.findIntervalForKill("test", 10000)
    );
  }
}
