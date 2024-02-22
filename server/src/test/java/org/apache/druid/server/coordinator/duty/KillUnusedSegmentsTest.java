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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorker;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.TestDruidCoordinatorConfig;
import org.apache.druid.server.coordinator.simulate.TestSegmentsMetadataManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Unit tests to test the {@link KillUnusedSegments} duty.
 */
public class KillUnusedSegmentsTest
{
  private static final Logger log = new Logger(KillUnusedSegmentsTest.class);
  private static final Duration INDEXING_PERIOD = Duration.standardSeconds(0);
  private static final Duration COORDINATOR_KILL_PERIOD = Duration.standardSeconds(0);
  private static final Duration DURATION_TO_RETAIN = Duration.standardHours(36);
  private static final Duration BUFFER_PERIOD = Duration.standardSeconds(1);
  private static final int MAX_SEGMENTS_TO_KILL = 10;
  private static final String DS1 = "DS1";
  private static final String DS2 = "DS2";

  private TestSegmentsMetadataManager segmentsMetadataManager;

  private TestOverlordClient overlordClient;

  private TestDruidCoordinatorConfig.Builder configBuilder;

  private DruidCoordinatorRuntimeParams.Builder paramsBuilder;

  private final CoordinatorDynamicConfig.Builder dynamicConfigBuilder = CoordinatorDynamicConfig.builder();

  private static final DateTime NOW = DateTimes.nowUtc();
  private static final Interval YEAR_OLD = new Interval(Period.days(1), NOW.minusDays(365));
  private static final Interval MONTH_OLD = new Interval(Period.days(1), NOW.minusDays(30));
  private static final Interval DAY_OLD = new Interval(Period.days(1), NOW.minusDays(1));
  private static final Interval HOUR_OLD = new Interval(Period.days(1), NOW.minusHours(1));
  private static final Interval NEXT_DAY = new Interval(Period.days(1), NOW.plusDays(1));
  private static final Interval NEXT_MONTH = new Interval(Period.days(1), NOW.plusDays(30));

  private DataSegment yearOldSegment;
  private DataSegment monthOldSegment;
  private DataSegment dayOldSegment;
  private DataSegment hourOldSegment;
  private DataSegment nextDaySegment;
  private DataSegment nextMonthSegment;


  @Before
  public void setup()
  {
    segmentsMetadataManager = new TestSegmentsMetadataManager();
    overlordClient = new TestOverlordClient();

    // These two can definitely be part of setup()
    configBuilder = new TestDruidCoordinatorConfig.Builder()
        .withCoordinatorIndexingPeriod(INDEXING_PERIOD)
        .withCoordinatorKillPeriod(COORDINATOR_KILL_PERIOD)
        .withCoordinatorKillDurationToRetain(DURATION_TO_RETAIN)
        .withCoordinatorKillMaxSegments(MAX_SEGMENTS_TO_KILL)
        .withCoordinatorKillBufferPeriod(BUFFER_PERIOD);
    paramsBuilder = DruidCoordinatorRuntimeParams.newBuilder(DateTimes.nowUtc());
  }

  /**
   * The buffer periood and duration to retain influence kill behavior.
   */
  @Test
  public void testDefaults()
  {
    final DateTime sixtyDaysAgo = NOW.minusDays(60);

    createAndAddUnusedSegment(DS1, YEAR_OLD, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, MONTH_OLD, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, DAY_OLD, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, HOUR_OLD, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, NEXT_DAY, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, NEXT_MONTH, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, Intervals.ETERNITY, sixtyDaysAgo);

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        new TestDruidCoordinatorConfig.Builder().build()
    );

    final DruidCoordinatorRuntimeParams firstRun = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 1L)),
        firstRun.getCoordinatorStats()
    );

    validateAndResetState(DS1, YEAR_OLD);
  }

  @Test
  public void testRunWithNoIntervalShouldNotKillAnySegments()
  {
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(10, 0, 10, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );

    validateAndResetState(DS1, null);
    validateAndResetState(DS2, null);
  }

  @Test
  public void testRunWithSpecificDatasourceAndNoIntervalShouldNotKillAnySegments()
  {
    configBuilder.withCoordinatorKillDurationToRetain(Duration.standardDays(400));
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(10, 0, 10, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );

    validateAndResetState(DS1, null);
    validateAndResetState(DS2, null);
  }

  /**
   * The kill period is honored after the first indexing run.
   */
  @Test
  public void testRunsWithKillPeriod()
  {
    configBuilder.withCoordinatorKillPeriod(Duration.standardHours(1));

    setupUnusedSegments();
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams firstRun = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 2L)),
        firstRun.getCoordinatorStats()
    );

    validateAndResetState(
        DS1,
        new Interval(
            yearOldSegment.getInterval().getStart(),
            monthOldSegment.getInterval().getEnd()
        )
    );

    final DruidCoordinatorRuntimeParams secondRun = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(10, 1, 10,  ImmutableMap.of(DS1, 2L)),
        secondRun.getCoordinatorStats()
    );
    validateAndResetState(DS1, null);
  }

  /**
   * Similar to {@link #testMultipleRuns()}
   */
  @Test
  public void testMultipleDatasources()
  {
    configBuilder.withCoordinatorKillIgnoreDurationToRetain(true);
    configBuilder.withCoordinatorKillMaxSegments(2);

    createAndAddUnusedSegment(DS1, YEAR_OLD, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, MONTH_OLD, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, DAY_OLD, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_DAY, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, NOW.minusDays(1));

    createAndAddUnusedSegment(DS2, YEAR_OLD, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, DAY_OLD, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, NEXT_DAY, NOW.minusDays(1));

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams firstKill = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 2, 10, ImmutableMap.of(DS1, 2L, DS2, 2L)),
        firstKill.getCoordinatorStats()
    );

    validateAndResetState(DS1, new Interval(YEAR_OLD.getStart(), MONTH_OLD.getEnd()));
    validateAndResetState(DS2, new Interval(YEAR_OLD.getStart(), DAY_OLD.getEnd()));

    final DruidCoordinatorRuntimeParams secondKill = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(20, 4, 20,  ImmutableMap.of(DS1, 4L, DS2, 3L)),
        secondKill.getCoordinatorStats()
    );

    validateAndResetState(DS1, new Interval(DAY_OLD.getStart(), NEXT_DAY.getEnd()));
    validateAndResetState(DS2, NEXT_DAY);

    final DruidCoordinatorRuntimeParams thirdKill = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(30, 5, 30, ImmutableMap.of(DS1, 5L, DS2, 3L)),
        thirdKill.getCoordinatorStats()
    );

    validateAndResetState(DS1, NEXT_MONTH);
    validateAndResetState(DS2, null);
  }

  /**
   * Even though "more recent" segments are also considered as candidates in the wide kill interval here,
   * the kill task will narrow down to clean up only the segments that strictly honor the max kill time.
   * See {@code KillUnusedSegmentsTaskTest#testKillMultipleUnusedSegmentsWithNullMaxUsedStatusLastUpdatedTime}
   * for example.
   */
  @Test
  public void testSpreadOutLastMaxSegments()
  {
    configBuilder.withCoordinatorKillIgnoreDurationToRetain(true);
    configBuilder.withCoordinatorKillBufferPeriod(Duration.standardDays(3));

    createAndAddUnusedSegment(DS1, YEAR_OLD, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, HOUR_OLD, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, NEXT_DAY, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, NOW.minusDays(10));

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams firstKill = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 4L)),
        firstKill.getCoordinatorStats()
    );
    validateAndResetState(DS1, new Interval(YEAR_OLD.getStart(), NEXT_MONTH.getEnd()));
  }

  @Test
  public void testAddOldVersionsLater()
  {
    configBuilder.withCoordinatorKillIgnoreDurationToRetain(true);
    configBuilder.withCoordinatorKillMaxSegments(2);

    createAndAddUnusedSegment(DS1, DAY_OLD, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_DAY, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, NOW.minusDays(1));

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams firstKill = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 2L)),
        firstKill.getCoordinatorStats()
    );

    validateAndResetState(DS1, new Interval(DAY_OLD.getStart(), NEXT_DAY.getEnd()));

    log.info("Second run...");
    // Add two old unused segments now. These only get killed much later on when the kill
    // duty eventually round robins its way through until the latest time.
    createAndAddUnusedSegment(DS1, YEAR_OLD, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, MONTH_OLD, NOW.minusDays(1));

    final DruidCoordinatorRuntimeParams secondKill = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(20, 2, 20, ImmutableMap.of(DS1, 3L)),
        secondKill.getCoordinatorStats()
    );

    validateAndResetState(DS1, NEXT_MONTH);

    log.info("Third run...");
    final DruidCoordinatorRuntimeParams thirdKill = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(30, 2, 30, ImmutableMap.of(DS1, 3L)),
        thirdKill.getCoordinatorStats()
    );

    validateAndResetState(DS1, null);

    log.info("Fourth run...");

    final DruidCoordinatorRuntimeParams fourthKill = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(40, 3, 40, ImmutableMap.of(DS1, 5L)),
        fourthKill.getCoordinatorStats()
    );

    validateAndResetState(DS1, new Interval(YEAR_OLD.getStart(), MONTH_OLD.getEnd()));
  }

  @Test
  public void testNoDatasources()
  {
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 0, 10, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );

    validateAndResetState(DS1, null);
    validateAndResetState(DS2, null);
  }

  @Test
  public void testWhiteList()
  {
    // Only segments more than a day old are killed
    dynamicConfigBuilder.withSpecificDataSourcesToKillUnusedSegmentsIn(Collections.singleton(DS2));
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    createAndAddUnusedSegment(DS1, YEAR_OLD, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, YEAR_OLD, NOW.minusDays(1));

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 0L, DS2, 1L)),
        runParams.getCoordinatorStats()
    );

    validateAndResetState(DS2, YEAR_OLD);
    validateAndResetState(DS1, null);
  }

  @Test
  public void testDurationToRetain()
  {
    // Only segments more than a day old are killed
    setupUnusedSegments();

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 2L)),
        runParams.getCoordinatorStats()
    );

    validateAndResetState(
        DS1,
        new Interval(
            yearOldSegment.getInterval().getStart(), monthOldSegment.getInterval().getEnd())
    );
  }

  @Test
  public void testNegativeDurationToRetain()
  {
    configBuilder.withCoordinatorKillDurationToRetain(DURATION_TO_RETAIN.negated());

    setupUnusedSegments();

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 5L)),
        runParams.getCoordinatorStats()
    );

    validateAndResetState(
        DS1,
        new Interval(yearOldSegment.getInterval().getStart(), nextDaySegment.getInterval().getEnd())
    );
  }

  @Test
  public void testIgnoreDurationToRetain()
  {
    configBuilder.withCoordinatorKillIgnoreDurationToRetain(true);

    setupUnusedSegments();

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 6L)),
        runParams.getCoordinatorStats()
    );

    // All past and future unused segments should be killed
    validateAndResetState(
        DS1,
        new Interval(yearOldSegment.getInterval().getStart(), nextMonthSegment.getInterval().getEnd())
    );
  }

  @Test
  public void testMaxSegmentsToKill()
  {
    configBuilder.withCoordinatorKillMaxSegments(1);

    setupUnusedSegments();

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 1L)),
        runParams.getCoordinatorStats()
    );

    validateAndResetState(
        DS1,
        yearOldSegment.getInterval()
    );
  }

  @Test
  public void testMultipleRuns()
  {
    configBuilder.withCoordinatorKillIgnoreDurationToRetain(true);
    configBuilder.withCoordinatorKillMaxSegments(2);

    setupUnusedSegments();

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    log.info("First run...");
    final DruidCoordinatorRuntimeParams firstRun = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 2L)),
        firstRun.getCoordinatorStats()
    );

    validateAndResetState(
        DS1,
        new Interval(
            yearOldSegment.getInterval().getStart(),
            monthOldSegment.getInterval().getEnd()
        )
    );

    log.info("Second run...");
    final DruidCoordinatorRuntimeParams secondRun = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(20, 2, 20, ImmutableMap.of(DS1, 4L)),
        secondRun.getCoordinatorStats()
    );

    validateAndResetState(
        DS1,
        new Interval(
            dayOldSegment.getInterval().getStart(),
            hourOldSegment.getInterval().getEnd()
        )
    );

    log.info("Third run...");
    final DruidCoordinatorRuntimeParams thirdRun = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(30, 3, 30, ImmutableMap.of(DS1, 6L)),
        thirdRun.getCoordinatorStats()
    );

    validateAndResetState(
        DS1,
        new Interval(
            nextDaySegment.getInterval().getStart(),
            nextMonthSegment.getInterval().getEnd()
        )
    );
  }

  @Test
  public void testKillTaskSlotRatioNoAvailableTaskCapacityForKill()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(0.10);
    dynamicConfigBuilder.withMaxKillTaskSlots(10);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    overlordClient = new TestOverlordClient(1, 5);

    overlordClient.addTask(DS1);

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(0, 0, 0, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );
    validateAndResetState(DS1, null);
  }

  @Test
  public void testMaxKillTaskSlotsNoAvailableTaskCapacityForKill()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(1.0);
    dynamicConfigBuilder.withMaxKillTaskSlots(3);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    overlordClient = new TestOverlordClient(3, 10);
    overlordClient.addTask(DS1);
    overlordClient.addTask(DS1);
    overlordClient.addTask(DS1);

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(0, 0, 3, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );
    validateAndResetState(DS1, null);
  }

  @Test
  public void testKillTaskSlotStats1()
  {
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );
    dynamicConfigBuilder.withKillTaskSlotRatio(1.0);
    dynamicConfigBuilder.withMaxKillTaskSlots(Integer.MAX_VALUE);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(10, 0, 10, ImmutableMap.of()),
        runParams.getCoordinatorStats());
  }

  @Test
  public void testKillTaskSlotStats2()
  {
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );
    dynamicConfigBuilder.withKillTaskSlotRatio(0.0);
    dynamicConfigBuilder.withMaxKillTaskSlots(Integer.MAX_VALUE);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(0, 0, 0, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );
  }

  @Test
  public void testKillTaskSlotStats3()
  {
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );
    dynamicConfigBuilder.withKillTaskSlotRatio(1.0);
    dynamicConfigBuilder.withMaxKillTaskSlots(0);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(0, 0, 0, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );
  }

  @Test
  public void testKillTaskSlotStats4()
  {
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );
    dynamicConfigBuilder.withKillTaskSlotRatio(0.1);
    dynamicConfigBuilder.withMaxKillTaskSlots(3);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(1, 0, 1, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );
  }

  @Test
  public void testKillTaskSlotStats5()
  {
    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );
    dynamicConfigBuilder.withKillTaskSlotRatio(0.3);
    dynamicConfigBuilder.withMaxKillTaskSlots(2);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    final DruidCoordinatorRuntimeParams runParams = killDuty.run(paramsBuilder.build());
    validateStats(
        new ExpectedStats(2, 0, 2, ImmutableMap.of()),
        runParams.getCoordinatorStats()
    );
  }

  @Test
  public void testKillFirstHalfEternitySegment()
  {
    final DateTime sixtyDaysAgo = NOW.minusDays(60);

    configBuilder.withCoordinatorKillIgnoreDurationToRetain(true);

    final Interval firstHalfEternity = new Interval(DateTimes.MIN, DateTimes.of("2024"));
    createAndAddUnusedSegment(DS1, firstHalfEternity, sixtyDaysAgo);

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams firstRun = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of()),
        firstRun.getCoordinatorStats()
    );

    validateAndResetState(DS1, firstHalfEternity);
  }

  /**
   * Regardless of ignoreDurationToRetain configuration, auto-kill doesn't delete unused eternity segments because the
   * unused segment retrieval code uses {@link DateTimes#COMPARE_DATE_AS_STRING_MAX} as the datetime string comparison
   * for the end endpoint when retrieving unused segment intervals.
   * TODO: link GitHub issue
   */
  @Ignore
  @Test
  public void testKillEternitySegment()
  {
    final DateTime sixtyDaysAgo = NOW.minusDays(60);

    configBuilder.withCoordinatorKillIgnoreDurationToRetain(true);

    createAndAddUnusedSegment(DS1, Intervals.ETERNITY, sixtyDaysAgo);

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams firstRun = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 1L)),
        firstRun.getCoordinatorStats()
    );

    validateAndResetState(DS1, Intervals.ETERNITY);
  }

  /**
   * Similar to {@link #testKillEternitySegment()}
   * <p>
   * Regardless of ignoreDurationToRetain configuration, auto-kill doesn't delete unused segments with an interval end with
   * {@link DateTimes#MAX}. This is because the kill duty uses {@link DateTimes#COMPARE_DATE_AS_STRING_MAX} as the
   * datetime string comparison for the end endpoint when retrieving unused segment intervals.
   * TODO: link GitHub issue
   * </p>
   */
  @Ignore
  @Test
  public void testKillSecondHalfEternitySegments()
  {
    final DateTime sixtyDaysAgo = NOW.minusDays(60);

    configBuilder.withCoordinatorKillIgnoreDurationToRetain(true);
    final Interval secondHalfEternity = new Interval(DateTimes.of("1970"), DateTimes.MAX);

    createAndAddUnusedSegment(DS1, secondHalfEternity, sixtyDaysAgo);

    final KillUnusedSegments killDuty = new KillUnusedSegments(
        segmentsMetadataManager,
        overlordClient,
        configBuilder.build()
    );

    final DruidCoordinatorRuntimeParams firstRun = killDuty.run(paramsBuilder.build());

    validateStats(
        new ExpectedStats(10, 1, 10, ImmutableMap.of(DS1, 1L)),
        firstRun.getCoordinatorStats()
    );

    validateAndResetState(DS1, secondHalfEternity);
  }

  private void validateStats(final ExpectedStats expectedStats, final CoordinatorRunStats actualRunStats)
  {
    Assert.assertEquals(expectedStats.availableSlots, actualRunStats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(expectedStats.submittedTasks, actualRunStats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(expectedStats.maxSlots, actualRunStats.get(Stats.Kill.MAX_SLOTS));

    for (final Map.Entry<String, Long> expectedEntry : expectedStats.dataSourceToCandidateSegments.entrySet()) {
      Assert.assertEquals(
          expectedEntry.getKey(),
          expectedEntry.getValue().longValue(),
          actualRunStats.get(
              Stats.Kill.CANDIDATE_SEGMENTS_KILLED,
              RowKey.of(Dimension.DATASOURCE, expectedEntry.getKey())
          )
      );
    }
  }

  private void validateAndResetState(final String dataSource, @Nullable final Interval expectedKillInterval)
  {
    final Interval observedLastKillInterval = overlordClient.getLastKillInterval(dataSource);
    final String observedLastKillTaskId = overlordClient.getLastKillTaskId(dataSource);

    Assert.assertEquals(
        expectedKillInterval,
        observedLastKillInterval
    );

    String expectedKillTaskId = null;
    if (expectedKillInterval != null) {
      expectedKillTaskId = TestOverlordClient.getTaskId(
          KillUnusedSegments.TASK_ID_PREFIX,
          dataSource,
          expectedKillInterval
      );
    }

    Assert.assertEquals(
        expectedKillTaskId,
        observedLastKillTaskId
    );

    // Clear the state after validation
    overlordClient.deleteLastKillTaskId(dataSource);
    overlordClient.deleteLastKillInterval(dataSource);
  }

  private DataSegment createSegmentWithInterval(final String dataSource, final Interval interval)
  {
    return new DataSegment(
        dataSource,
        interval,
        NOW.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        1,
        0
    );
  }

  private DataSegment createSegmentWithEnd(final String dataSource, final DateTime endTime)
  {
    return new DataSegment(
        dataSource,
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

  private void setupUnusedSegments()
  {
    final DateTime endTime = DateTimes.nowUtc();
    final DateTime lastUpdatedTime = endTime.minus(Duration.standardDays(1));

    setupUnusedSegments(DS1, endTime, lastUpdatedTime);
  }

  private void setupUnusedSegments(
      final String dataSource,
      final DateTime endTime,
      final DateTime lastUpdatedTime
  )
  {
    // TODO: maybe move this to setup and have the tests just pass segment and last updated time.
    final DateTime now = DateTimes.nowUtc();

    yearOldSegment = createSegmentWithEnd(dataSource, now.minusDays(365));
    monthOldSegment = createSegmentWithEnd(dataSource, now.minusDays(30));
    dayOldSegment = createSegmentWithEnd(dataSource, now.minusDays(1));
    hourOldSegment = createSegmentWithEnd(dataSource, now.minusHours(1));
    nextDaySegment = createSegmentWithEnd(dataSource, now.plusDays(1));
    nextMonthSegment = createSegmentWithEnd(dataSource, now.plusDays(30));

    DataSegmentPlus plus1 = new DataSegmentPlus(yearOldSegment, now, lastUpdatedTime);
    DataSegmentPlus plus2 = new DataSegmentPlus(monthOldSegment, now, lastUpdatedTime);
    DataSegmentPlus plus3 = new DataSegmentPlus(dayOldSegment, now, lastUpdatedTime);
    DataSegmentPlus plus4 = new DataSegmentPlus(hourOldSegment, now, lastUpdatedTime);
    DataSegmentPlus plus5 = new DataSegmentPlus(nextDaySegment, now, lastUpdatedTime);
    DataSegmentPlus plus6 = new DataSegmentPlus(nextMonthSegment, now, lastUpdatedTime);

    segmentsMetadataManager.addUnusedSegment(plus1);
    segmentsMetadataManager.addUnusedSegment(plus2);
    segmentsMetadataManager.addUnusedSegment(plus3);
    segmentsMetadataManager.addUnusedSegment(plus4);
    segmentsMetadataManager.addUnusedSegment(plus5);
    segmentsMetadataManager.addUnusedSegment(plus6);
  }

  private DataSegmentPlus createAndAddUnusedSegment(
      final String dataSource,
      final Interval interval,
      final DateTime lastUpdatedTime
  )
  {
    final DataSegment segment = createSegmentWithInterval(dataSource, interval);
    final DataSegmentPlus unusedSegmentPlus = new DataSegmentPlus(
        segment,
        DateTimes.nowUtc(),
        lastUpdatedTime
    );
    segmentsMetadataManager.addUnusedSegment(unusedSegmentPlus);
    return unusedSegmentPlus;
  }

  private static class ExpectedStats
  {
    private final int availableSlots;
    private final int submittedTasks;
    private final int maxSlots;
    private final Map<String, Long> dataSourceToCandidateSegments = new HashMap<>();

    ExpectedStats(int availableSlots, int submittedTasks, int maxSlots, Map<String, Long> dataSourceToCandidateSegments)
    {
      this.availableSlots = availableSlots;
      this.submittedTasks = submittedTasks;
      this.maxSlots = maxSlots;
      this.dataSourceToCandidateSegments.putAll(dataSourceToCandidateSegments);
    }
  }

  /**
   * Simulates an Overlord with a configurable list of kill tasks. It also helps verify the calls made by the kill duty.
   */
  private static class TestOverlordClient extends NoopOverlordClient
  {
    private final List<TaskStatusPlus> taskStatuses = new ArrayList<>();

    private final Map<String, Interval> observedDatasourceToLastKillInterval = new HashMap<>();
    private final Map<String, String> observedDatasourceToLastKillTaskId = new HashMap<>();

    private int taskIdSuffix = 0;

    private final IndexingTotalWorkerCapacityInfo capcityInfo;

    TestOverlordClient()
    {
      capcityInfo = new IndexingTotalWorkerCapacityInfo(5, 10);
    }

    TestOverlordClient(final int currentClusterCapacity, final int maxWorkerCapacity)
    {
      capcityInfo = new IndexingTotalWorkerCapacityInfo(currentClusterCapacity, maxWorkerCapacity);
    }

    void addTask(final String datasource)
    {
      taskStatuses.add(
          new TaskStatusPlus(
              KillUnusedSegments.TASK_ID_PREFIX + "__" + datasource + "__" + taskIdSuffix++,
              null,
              KillUnusedSegments.KILL_TASK_TYPE,
              DateTimes.EPOCH,
              DateTimes.EPOCH,
              TaskState.RUNNING,
              RunnerTaskState.RUNNING,
              100L,
              TaskLocation.unknown(),
              datasource,
              null
          )
      );
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      return Futures.immediateFuture(
          CloseableIterators.wrap(taskStatuses.iterator(), null)
      );
    }

    @Override
    public ListenableFuture<String> runKillTask(
        String idPrefix,
        String dataSource,
        Interval interval,
        @Nullable Integer maxSegmentsToKill,
        @Nullable DateTime maxUsedStatusLastUpdatedTime
    )
    {
      observedDatasourceToLastKillInterval.put(dataSource, interval);
      final String taskId = getTaskId(idPrefix, dataSource, interval);
      observedDatasourceToLastKillTaskId.put(dataSource, taskId);
      log.info(
          "Creating taskId[%s] -- Setting kill task interval for ds[%s] : interval[%s]",
          taskId,
          dataSource,
          interval
      );
      return Futures.immediateFuture(taskId);
    }

    @Override
    public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
    {
      return Futures.immediateFuture(capcityInfo);
    }

    @Override
    public ListenableFuture<List<IndexingWorkerInfo>> getWorkers()
    {
      return Futures.immediateFuture(
          ImmutableList.of(
              new IndexingWorkerInfo(
                  new IndexingWorker("http", "localhost", "1.2.3.4", 3, "2"),
                  0,
                  Collections.emptySet(),
                  Collections.emptyList(),
                  DateTimes.of("2000"),
                  null
              )
          )
      );
    }

    Interval getLastKillInterval(final String dataSource)
    {
      return observedDatasourceToLastKillInterval.get(dataSource);
    }

    void deleteLastKillInterval(final String dataSource)
    {
      observedDatasourceToLastKillInterval.remove(dataSource);
    }

    String getLastKillTaskId(final String dataSource)
    {
      final String lastKillTaskId = observedDatasourceToLastKillTaskId.get(dataSource);
      observedDatasourceToLastKillTaskId.remove(dataSource);
      return lastKillTaskId;
    }

    void deleteLastKillTaskId(final String dataSource)
    {
      observedDatasourceToLastKillTaskId.remove(dataSource);
    }

    static String getTaskId(final String idPrefix, final String dataSource, final Interval interval)
    {
      return idPrefix + "-" + dataSource + "-" + interval;
    }
  }
}
