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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
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
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class KillUnusedSegmentsTest
{
  private static final Duration INDEXING_PERIOD = Duration.standardSeconds(1);
  private static final Duration COORDINATOR_KILL_PERIOD = Duration.standardSeconds(1);
  private static final Duration DURATION_TO_RETAIN = Duration.standardDays(1);
  private static final int MAX_SEGMENTS_TO_KILL = 10;
  private static final String DATASOURCE = "DS1";

  @Mock
  private SegmentsMetadataManager segmentsMetadataManager;
  @Mock
  private OverlordClient overlordClient;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private DruidCoordinatorConfig config;

  @Mock
  private CoordinatorRunStats stats;
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
    Mockito.doReturn(stats).when(params).getCoordinatorStats();
    Mockito.doReturn(COORDINATOR_KILL_PERIOD).when(config).getCoordinatorKillPeriod();
    Mockito.doReturn(DURATION_TO_RETAIN).when(config).getCoordinatorKillDurationToRetain();
    Mockito.doReturn(INDEXING_PERIOD).when(config).getCoordinatorIndexingPeriod();
    Mockito.doReturn(MAX_SEGMENTS_TO_KILL).when(config).getCoordinatorKillMaxSegments();
    Mockito.doReturn(Duration.parse("PT3154000000S")).when(config).getCoordinatorKillBufferPeriod();

    Mockito.doReturn(Collections.singleton(DATASOURCE))
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
            ArgumentMatchers.any(),
            ArgumentMatchers.anyInt(),
            ArgumentMatchers.any()
        )
    ).thenAnswer(invocation -> {
      DateTime minStartTime = invocation.getArgument(1);
      DateTime maxEndTime = invocation.getArgument(2);
      long maxEndMillis = maxEndTime.getMillis();
      Long minStartMillis = minStartTime != null ? minStartTime.getMillis() : null;
      List<Interval> unusedIntervals =
          unusedSegments.stream()
                        .map(DataSegment::getInterval)
                        .filter(i -> i.getEnd().getMillis() <= maxEndMillis
                                     && (null == minStartMillis || i.getStart().getMillis() >= minStartMillis))
                        .collect(Collectors.toList());

      int limit = invocation.getArgument(3);
      return unusedIntervals.size() <= limit ? unusedIntervals : unusedIntervals.subList(0, limit);
    });

    target = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        config
    );
  }

  @Test
  public void testRunWithNoIntervalShouldNotKillAnySegments()
  {
    Mockito.doReturn(null).when(segmentsMetadataManager).getUnusedSegmentIntervals(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.anyInt(),
        ArgumentMatchers.any()
    );

    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    target.run(params);
    Mockito.verify(overlordClient, Mockito.never())
           .runKillTask(anyString(), anyString(), any(Interval.class), anyInt(), any(DateTime.class));
  }

  @Test
  public void testRunWithSpecificDatasourceAndNoIntervalShouldNotKillAnySegments()
  {
    Mockito.doReturn(Duration.standardDays(400))
           .when(config).getCoordinatorKillDurationToRetain();
    target = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        config
    );

    // No unused segment is older than the retention period
    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    target.run(params);
    Mockito.verify(overlordClient, Mockito.never())
           .runKillTask(anyString(), anyString(), any(Interval.class), anyInt(), any(DateTime.class));
  }

  @Test
  public void testDurationToRetain()
  {
    // Only segments more than a day old are killed
    Interval expectedKillInterval = new Interval(
        yearOldSegment.getInterval().getStart(),
        dayOldSegment.getInterval().getEnd()
    );
    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    runAndVerifyKillInterval(expectedKillInterval);
    verifyState(ImmutableMap.of(DATASOURCE, dayOldSegment.getInterval().getEnd()));
    verifyStats(9, 1, 10);
  }

  @Test
  public void testNegativeDurationToRetain()
  {
    // Duration to retain = -1 day, reinit target for config to take effect
    Mockito.doReturn(DURATION_TO_RETAIN.negated())
           .when(config).getCoordinatorKillDurationToRetain();
    target = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        config
    );

    // Segments upto 1 day in the future are killed
    Interval expectedKillInterval = new Interval(
        yearOldSegment.getInterval().getStart(),
        nextDaySegment.getInterval().getEnd()
    );
    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    runAndVerifyKillInterval(expectedKillInterval);
    verifyState(ImmutableMap.of(DATASOURCE, nextDaySegment.getInterval().getEnd()));
    verifyStats(9, 1, 10);
  }

  @Test
  public void testIgnoreDurationToRetain()
  {
    Mockito.doReturn(true)
           .when(config).getCoordinatorKillIgnoreDurationToRetain();
    target = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        config
    );

    // All future and past unused segments are killed
    Interval expectedKillInterval = new Interval(
        yearOldSegment.getInterval().getStart(),
        nextMonthSegment.getInterval().getEnd()
    );
    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    runAndVerifyKillInterval(expectedKillInterval);
    verifyState(ImmutableMap.of(DATASOURCE, nextMonthSegment.getInterval().getEnd()));
    verifyStats(9, 1, 10);
  }

  @Test
  public void testMaxSegmentsToKill()
  {
    Mockito.doReturn(1)
           .when(config).getCoordinatorKillMaxSegments();
    target = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        config
    );

    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    // Only 1 unused segment is killed
    runAndVerifyKillInterval(yearOldSegment.getInterval());
    verifyState(ImmutableMap.of(DATASOURCE, yearOldSegment.getInterval().getEnd()));
    verifyStats(9, 1, 10);
  }

  @Test
  public void testMultipleRuns()
  {
    Mockito.doReturn(true)
        .when(config).getCoordinatorKillIgnoreDurationToRetain();
    Mockito.doReturn(2)
        .when(config).getCoordinatorKillMaxSegments();
    target = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        config
    );

    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    runAndVerifyKillInterval(new Interval(
        yearOldSegment.getInterval().getStart(),
        monthOldSegment.getInterval().getEnd()
    ));
    verifyState(ImmutableMap.of(DATASOURCE, monthOldSegment.getInterval().getEnd()));

    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    runAndVerifyKillInterval(new Interval(
        dayOldSegment.getInterval().getStart(),
        hourOldSegment.getInterval().getEnd()
    ));
    verifyState(ImmutableMap.of(DATASOURCE, hourOldSegment.getInterval().getEnd()));

    mockTaskSlotUsage(1.0, Integer.MAX_VALUE, 1, 10);
    runAndVerifyKillInterval(new Interval(
        nextDaySegment.getInterval().getStart(),
        nextMonthSegment.getInterval().getEnd()
    ));
    verifyState(ImmutableMap.of(DATASOURCE, nextMonthSegment.getInterval().getEnd()));
    verifyStats(9, 1, 10, 3);
  }

  @Test
  public void testKillTaskSlotRatioNoAvailableTaskCapacityForKill()
  {
    mockTaskSlotUsage(0.10, 10, 1, 5);
    runAndVerifyNoKill();
    verifyState(ImmutableMap.of());
    verifyStats(0, 0, 0);
  }

  @Test
  public void testMaxKillTaskSlotsNoAvailableTaskCapacityForKill()
  {
    mockTaskSlotUsage(1.0, 3, 3, 10);
    runAndVerifyNoKill();
    verifyState(ImmutableMap.of());
    verifyStats(0, 0, 3);
  }

  @Test
  public void testGetKillTaskCapacity()
  {
    Assert.assertEquals(
        10,
        KillUnusedSegments.getKillTaskCapacity(10, 1.0, Integer.MAX_VALUE)
    );

    Assert.assertEquals(
        0,
        KillUnusedSegments.getKillTaskCapacity(10, 0.0, Integer.MAX_VALUE)
    );

    Assert.assertEquals(
        10,
        KillUnusedSegments.getKillTaskCapacity(10, Double.POSITIVE_INFINITY, Integer.MAX_VALUE)
    );

    Assert.assertEquals(
        0,
        KillUnusedSegments.getKillTaskCapacity(10, 1.0, 0)
    );

    Assert.assertEquals(
        1,
        KillUnusedSegments.getKillTaskCapacity(10, 0.1, 3)
    );

    Assert.assertEquals(
        2,
        KillUnusedSegments.getKillTaskCapacity(10, 0.3, 2)
    );
  }

  private void runAndVerifyKillInterval(Interval expectedKillInterval)
  {
    int limit = config.getCoordinatorKillMaxSegments();
    Mockito.doReturn(Futures.immediateFuture("ok"))
           .when(overlordClient)
           .runKillTask(
               ArgumentMatchers.anyString(),
               ArgumentMatchers.anyString(),
               ArgumentMatchers.any(Interval.class),
               ArgumentMatchers.anyInt(),
               ArgumentMatchers.any(DateTime.class));
    target.runInternal(params);

    Mockito.verify(overlordClient, Mockito.times(1)).runKillTask(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(DATASOURCE),
        ArgumentMatchers.eq(expectedKillInterval),
        ArgumentMatchers.eq(limit),
        ArgumentMatchers.any()
    );
  }

  private void verifyStats(int availableSlots, int submittedTasks, int maxSlots)
  {
    verifyStats(availableSlots, submittedTasks, maxSlots, 1);
  }

  private void verifyStats(int availableSlots, int submittedTasks, int maxSlots, int times)
  {
    Mockito.verify(stats, Mockito.times(times)).add(Stats.Kill.AVAILABLE_SLOTS, availableSlots);
    Mockito.verify(stats, Mockito.times(times)).add(Stats.Kill.SUBMITTED_TASKS, submittedTasks);
    Mockito.verify(stats, Mockito.times(times)).add(Stats.Kill.MAX_SLOTS, maxSlots);
  }

  private void verifyState(Map<String, DateTime> expectedDatasourceToLastKillIntervalEnd)
  {
    Assert.assertEquals(expectedDatasourceToLastKillIntervalEnd, target.getDatasourceToLastKillIntervalEnd());
  }

  private void runAndVerifyNoKill()
  {
    target.run(params);
    Mockito.verify(overlordClient, Mockito.never()).runKillTask(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.any(Interval.class),
        ArgumentMatchers.anyInt(),
        ArgumentMatchers.any(DateTime.class)
    );
  }

  private void mockTaskSlotUsage(
      double killTaskSlotRatio,
      int maxKillTaskSlots,
      int numPendingCoordKillTasks,
      int maxWorkerCapacity
  )
  {
    Mockito.doReturn(killTaskSlotRatio)
        .when(coordinatorDynamicConfig).getKillTaskSlotRatio();
    Mockito.doReturn(maxKillTaskSlots)
        .when(coordinatorDynamicConfig).getMaxKillTaskSlots();
    Mockito.doReturn(Futures.immediateFuture(new IndexingTotalWorkerCapacityInfo(1, maxWorkerCapacity)))
        .when(overlordClient)
        .getTotalWorkerCapacity();
    List<TaskStatusPlus> runningCoordinatorIssuedKillTasks = new ArrayList<>();
    for (int i = 0; i < numPendingCoordKillTasks; i++) {
      runningCoordinatorIssuedKillTasks.add(new TaskStatusPlus(
          KillUnusedSegments.TASK_ID_PREFIX + "_taskId_" + i,
          "groupId_" + i,
          KillUnusedSegments.KILL_TASK_TYPE,
          DateTimes.EPOCH,
          DateTimes.EPOCH,
          TaskState.RUNNING,
          RunnerTaskState.RUNNING,
          -1L,
          TaskLocation.unknown(),
          "datasource",
          null
      ));
    }
    Mockito.doReturn(Futures.immediateFuture(
            CloseableIterators.withEmptyBaggage(runningCoordinatorIssuedKillTasks.iterator())))
        .when(overlordClient)
        .taskStatuses(null, null, 0);
  }

  private DataSegment createSegmentWithEnd(DateTime endTime)
  {
    return new DataSegment(
        DATASOURCE,
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
