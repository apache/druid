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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.TestDruidCoordinatorConfig;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 */
@RunWith(Enclosed.class)
public class KillUnusedSegmentsTest
{
  /**
   * Standing up new tests with mocks was easier than trying to move the existing tests to use mocks for consistency.
   * In the future, if all tests are moved to use the same structure, this inner static class can be gotten rid of.
   */
  @RunWith(MockitoJUnitRunner.class)
  public static class MockedTest
  {
    private static final Set<String> ALL_DATASOURCES = ImmutableSet.of("DS1", "DS2", "DS3");
    private static final int MAX_SEGMENTS_TO_KILL = 10;
    private static final Duration COORDINATOR_KILL_PERIOD = Duration.standardMinutes(2);
    private static final Duration DURATION_TO_RETAIN = Duration.standardDays(1);
    private static final Duration INDEXING_PERIOD = Duration.standardMinutes(1);

    @Mock
    private SegmentsMetadataManager segmentsMetadataManager;
    @Mock
    private IndexingServiceClient indexingServiceClient;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DruidCoordinatorConfig config;

    @Mock
    private DruidCoordinatorRuntimeParams params;
    @Mock
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private KillUnusedSegments target;

    @Before
    public void setup()
    {
      Mockito.doReturn(coordinatorDynamicConfig).when(params).getCoordinatorDynamicConfig();
      Mockito.doReturn(ALL_DATASOURCES).when(segmentsMetadataManager).retrieveAllDataSourceNames();
      Mockito.doReturn(COORDINATOR_KILL_PERIOD).when(config).getCoordinatorKillPeriod();
      Mockito.doReturn(DURATION_TO_RETAIN).when(config).getCoordinatorKillDurationToRetain();
      Mockito.doReturn(INDEXING_PERIOD).when(config).getCoordinatorIndexingPeriod();
      Mockito.doReturn(MAX_SEGMENTS_TO_KILL).when(config).getCoordinatorKillMaxSegments();
      target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);
    }
    @Test
    public void testRunWihNoIntervalShouldNotKillAnySegments()
    {
      target.run(params);
      Mockito.verify(indexingServiceClient, Mockito.never())
             .killUnusedSegments(anyString(), anyString(), any(Interval.class));
    }

    @Test
    public void testRunWihSpecificDatasourceAndNoIntervalShouldNotKillAnySegments()
    {
      Mockito.when(coordinatorDynamicConfig.getSpecificDataSourcesToKillUnusedSegmentsIn()).thenReturn(Collections.singleton("DS1"));
      target.run(params);
      Mockito.verify(indexingServiceClient, Mockito.never())
             .killUnusedSegments(anyString(), anyString(), any(Interval.class));
    }
  }

  public static class FindIntervalsTest
  {
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
          new TestDruidCoordinatorConfig.Builder()
              .withCoordinatorIndexingPeriod(Duration.parse("PT76400S"))
              .withLoadTimeoutDelay(new Duration(1))
              .withCoordinatorKillPeriod(Duration.parse("PT86400S"))
              .withCoordinatorKillDurationToRetain(Duration.parse("PT86400S"))
              .withCoordinatorKillMaxSegments(1000)
              .withLoadQueuePeonRepeatDelay(Duration.ZERO)
              .withCoordinatorKillIgnoreDurationToRetain(false)
              .build()
      );

      Assert.assertEquals(
          expected,
          unusedSegmentsKiller.findIntervalForKill("test", 10000)
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
          new TestDruidCoordinatorConfig.Builder()
              .withCoordinatorIndexingPeriod(Duration.parse("PT76400S"))
              .withLoadTimeoutDelay(new Duration(1))
              .withCoordinatorKillPeriod(Duration.parse("PT86400S"))
              .withCoordinatorKillDurationToRetain(Duration.parse("PT86400S"))
              .withCoordinatorKillMaxSegments(1000)
              .withLoadQueuePeonRepeatDelay(Duration.ZERO)
              .withCoordinatorKillIgnoreDurationToRetain(false)
              .build()
      );
      Assert.assertEquals((Long) Duration.parse("PT86400S").getMillis(), unusedSegmentsKiller.getRetainDuration());

      // Negative duration to retain
      unusedSegmentsKiller = new KillUnusedSegments(
          null,
          null,
          new TestDruidCoordinatorConfig.Builder()
              .withCoordinatorIndexingPeriod(Duration.parse("PT76400S"))
              .withLoadTimeoutDelay(new Duration(1))
              .withCoordinatorKillPeriod(Duration.parse("PT86400S"))
              .withCoordinatorKillDurationToRetain(Duration.parse("PT-86400S"))
              .withCoordinatorKillMaxSegments(1000)
              .withLoadQueuePeonRepeatDelay(Duration.ZERO)
              .withCoordinatorKillIgnoreDurationToRetain(false)
              .build()
      );
      Assert.assertEquals((Long) Duration.parse("PT-86400S").getMillis(), unusedSegmentsKiller.getRetainDuration());
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
          new TestDruidCoordinatorConfig.Builder()
              .withCoordinatorIndexingPeriod(Duration.parse("PT76400S"))
              .withLoadTimeoutDelay(new Duration(1))
              .withCoordinatorKillPeriod(Duration.parse("PT86400S"))
              .withCoordinatorKillDurationToRetain(Duration.parse("PT86400S"))
              .withCoordinatorKillMaxSegments(1000)
              .withLoadQueuePeonRepeatDelay(Duration.ZERO)
              .withCoordinatorKillIgnoreDurationToRetain(true)
              .build()
      );
      Assert.assertEquals(
          DateTimes.COMPARE_DATE_AS_STRING_MAX,
          unusedSegmentsKiller.getEndTimeUpperLimit()
      );

      // Testing a negative durationToRetain period returns proper date in future
      unusedSegmentsKiller = new KillUnusedSegments(
          null,
          null,
          new TestDruidCoordinatorConfig.Builder()
              .withCoordinatorIndexingPeriod(Duration.parse("PT76400S"))
              .withLoadTimeoutDelay(new Duration(1))
              .withCoordinatorKillPeriod(Duration.parse("PT86400S"))
              .withCoordinatorKillDurationToRetain(Duration.parse("PT-86400S"))
              .withCoordinatorKillMaxSegments(1000)
              .withLoadQueuePeonRepeatDelay(Duration.ZERO)
              .withCoordinatorKillIgnoreDurationToRetain(false)
              .build()
      );

      DateTime expectedTime = DateTimes.nowUtc().minus(Duration.parse("PT-86400S").getMillis());
      Assert.assertEquals(expectedTime, unusedSegmentsKiller.getEndTimeUpperLimit());

      // Testing a positive durationToRetain period returns expected value in the past
      unusedSegmentsKiller = new KillUnusedSegments(
          null,
          null,
          new TestDruidCoordinatorConfig.Builder()
              .withCoordinatorIndexingPeriod(Duration.parse("PT76400S"))
              .withLoadTimeoutDelay(new Duration(1))
              .withCoordinatorKillPeriod(Duration.parse("PT86400S"))
              .withCoordinatorKillDurationToRetain(Duration.parse("PT86400S"))
              .withCoordinatorKillMaxSegments(1000)
              .withLoadQueuePeonRepeatDelay(Duration.ZERO)
              .withCoordinatorKillIgnoreDurationToRetain(false)
              .build()
      );
      expectedTime = DateTimes.nowUtc().minus(Duration.parse("PT86400S").getMillis());
      Assert.assertEquals(expectedTime, unusedSegmentsKiller.getEndTimeUpperLimit());
    }
  }
}
