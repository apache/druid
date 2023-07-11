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
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class KillUnusedSegmentsTest
{
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

  private DataSegment yearOldSegment;
  private DataSegment monthOldSegment;
  private DataSegment dayOldSegment;
  private DataSegment hourOldSegment;
  private DataSegment nextDaySegment;
  private DataSegment nextMonthSegment;

  private KillUnusedSegments target;

  @Before
  public void setup()
  {
    Mockito.doReturn(coordinatorDynamicConfig).when(params).getCoordinatorDynamicConfig();
    Mockito.doReturn(COORDINATOR_KILL_PERIOD).when(config).getCoordinatorKillPeriod();
    Mockito.doReturn(DURATION_TO_RETAIN).when(config).getCoordinatorKillDurationToRetain();
    Mockito.doReturn(INDEXING_PERIOD).when(config).getCoordinatorIndexingPeriod();
    Mockito.doReturn(MAX_SEGMENTS_TO_KILL).when(config).getCoordinatorKillMaxSegments();

    Mockito.doReturn(Collections.singleton("DS1"))
           .when(coordinatorDynamicConfig).getSpecificDataSourcesToKillUnusedSegmentsIn();

    final DateTime now = DateTimes.nowUtc();

    yearOldSegment = createSegmentWithEnd(now.minusDays(365));
    monthOldSegment = createSegmentWithEnd(now.minusDays(30));
    dayOldSegment = createSegmentWithEnd(now.minusDays(1));
    hourOldSegment = createSegmentWithEnd(now.minusHours(1));
    nextDaySegment = createSegmentWithEnd(now.plusDays(1));
    nextMonthSegment = createSegmentWithEnd(now.plusDays(30));

    final List<DataSegment> unusedSegments = ImmutableList.of(
        yearOldSegment,
        monthOldSegment,
        dayOldSegment,
        hourOldSegment,
        nextDaySegment,
        nextMonthSegment
    );

    Mockito.when(
        segmentsMetadataManager.getUnusedSegmentIntervals(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any(),
            ArgumentMatchers.anyInt()
        )
    ).thenAnswer(invocation -> {
      DateTime maxEndTime = invocation.getArgument(1);
      long maxEndMillis = maxEndTime.getMillis();
      List<Interval> unusedIntervals =
          unusedSegments.stream()
                        .map(DataSegment::getInterval)
                        .filter(i -> i.getEnd().getMillis() <= maxEndMillis)
                        .collect(Collectors.toList());

      int limit = invocation.getArgument(2);
      return unusedIntervals.size() <= limit ? unusedIntervals : unusedIntervals.subList(0, limit);
    });

    target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);
  }

  @Test
  public void testRunWithNoIntervalShouldNotKillAnySegments()
  {
    Mockito.doReturn(null).when(segmentsMetadataManager).getUnusedSegmentIntervals(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.any(),
        ArgumentMatchers.anyInt()
    );

    target.run(params);
    Mockito.verify(indexingServiceClient, Mockito.never())
           .killUnusedSegments(anyString(), anyString(), any(Interval.class));
  }

  @Test
  public void testRunWithSpecificDatasourceAndNoIntervalShouldNotKillAnySegments()
  {
    Mockito.doReturn(Duration.standardDays(400))
           .when(config).getCoordinatorKillDurationToRetain();
    target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);

    // No unused segment is older than the retention period
    target.run(params);
    Mockito.verify(indexingServiceClient, Mockito.never())
           .killUnusedSegments(anyString(), anyString(), any(Interval.class));
  }

  @Test
  public void testDurationToRetain()
  {
    // Only segments more than a day old are killed
    Interval expectedKillInterval = new Interval(
        yearOldSegment.getInterval().getStart(),
        dayOldSegment.getInterval().getEnd()
    );
    runAndVerifyKillInterval(expectedKillInterval);
  }

  @Test
  public void testNegativeDurationToRetain()
  {
    // Duration to retain = -1 day, reinit target for config to take effect
    Mockito.doReturn(DURATION_TO_RETAIN.negated())
           .when(config).getCoordinatorKillDurationToRetain();
    target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);

    // Segments upto 1 day in the future are killed
    Interval expectedKillInterval = new Interval(
        yearOldSegment.getInterval().getStart(),
        nextDaySegment.getInterval().getEnd()
    );
    runAndVerifyKillInterval(expectedKillInterval);
  }

  @Test
  public void testIgnoreDurationToRetain()
  {
    Mockito.doReturn(true)
           .when(config).getCoordinatorKillIgnoreDurationToRetain();
    target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);

    // All future and past unused segments are killed
    Interval expectedKillInterval = new Interval(
        yearOldSegment.getInterval().getStart(),
        nextMonthSegment.getInterval().getEnd()
    );
    runAndVerifyKillInterval(expectedKillInterval);
  }

  @Test
  public void testMaxSegmentsToKill()
  {
    Mockito.doReturn(1)
           .when(config).getCoordinatorKillMaxSegments();
    target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);

    // Only 1 unused segment is killed
    runAndVerifyKillInterval(yearOldSegment.getInterval());
  }

  private void runAndVerifyKillInterval(Interval expectedKillInterval)
  {
    target.run(params);
    Mockito.verify(indexingServiceClient, Mockito.times(1)).killUnusedSegments(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("DS1"),
        ArgumentMatchers.eq(expectedKillInterval)
    );
  }

  private DataSegment createSegmentWithEnd(DateTime endTime)
  {
    return new DataSegment(
        "DS1",
        new Interval(Period.days(1), endTime),
        DateTimes.nowUtc().toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        1,
        0
    );
  }
}
