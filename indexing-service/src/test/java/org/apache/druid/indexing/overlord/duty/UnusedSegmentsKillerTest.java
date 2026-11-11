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

package org.apache.druid.indexing.overlord.duty;

import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.RetrieveUpgradedToSegmentIdsAction;
import org.apache.druid.indexing.common.actions.TaskActionTestKit;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskMetrics;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.UnusedSegmentKillerConfig;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestDruidLeaderSelector;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UnusedSegmentsKillerTest
{
  private final TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private static final List<DataSegment> WIKI_SEGMENTS_1X10D =
      CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                        .forIntervals(10, Granularities.DAY)
                        .eachOfSize(500);

  private StubServiceEmitter emitter;
  private UnusedSegmentsKiller killer;
  private BlockingExecutorService killExecutor;
  private UnusedSegmentKillerConfig killerConfig;
  private TestDruidLeaderSelector leaderSelector;
  private TestDataSegmentKiller dataSegmentKiller;
  private IndexerMetadataStorageCoordinator storageCoordinator;

  @BeforeEach
  public void setup()
  {
    taskActionTestKit.before();
    emitter = taskActionTestKit.getServiceEmitter();
    leaderSelector = new TestDruidLeaderSelector();
    dataSegmentKiller = new TestDataSegmentKiller();
    killerConfig = new UnusedSegmentKillerConfig(true, zeroBufferPeriod(), null, null);
    killExecutor = new BlockingExecutorService("UnusedSegmentsKillerTest-%s");
    storageCoordinator = taskActionTestKit.getMetadataStorageCoordinator();
    initKiller();
  }

  @AfterEach
  public void tearDown()
  {
    taskActionTestKit.after();
  }

  private void initKiller()
  {
    killer = new UnusedSegmentsKiller(
        new SegmentsMetadataManagerConfig(
            null,
            SegmentMetadataCache.UsageMode.ALWAYS,
            killerConfig
        ),
        taskActionTestKit::createTaskActionClient,
        storageCoordinator,
        leaderSelector,
        (corePoolSize, nameFormat) -> new WrappingScheduledExecutorService(nameFormat, killExecutor, true),
        dataSegmentKiller,
        taskActionTestKit.getTaskLockbox(),
        taskActionTestKit.getServiceEmitter()
    );
  }

  private void resetKillQueue()
  {
    killer.run();

    // Invoke rebuild of kill queue on the executor thread
    killExecutor.finishNextPendingTask();
  }

  private void finishQueuedKillJobs()
  {
    killExecutor.finishAllPendingTasks();
  }

  @Test
  public void test_getSchedule_returnsOneHourPeriod_ifEnabled()
  {
    final DutySchedule schedule = killer.getSchedule();
    Assertions.assertEquals(Duration.standardHours(1).getMillis(), schedule.getPeriodMillis());
    Assertions.assertEquals(Duration.standardMinutes(30).getMillis(), schedule.getInitialDelayMillis());
  }

  @Test
  public void test_getSchedule_returnsZeroPeriod_ifDisabled()
  {
    killerConfig = new UnusedSegmentKillerConfig(false, null, null, null);
    initKiller();

    final DutySchedule schedule = killer.getSchedule();
    Assertions.assertEquals(0, schedule.getPeriodMillis());
    Assertions.assertEquals(0, schedule.getInitialDelayMillis());
  }

  @Test
  public void test_run_startsProcessing_ifEnabled()
  {
    Assertions.assertFalse(killExecutor.hasPendingTasks());
    Assertions.assertTrue(killer.isEnabled());

    killer.run();
    Assertions.assertTrue(killExecutor.hasPendingTasks());
  }

  @Test
  public void test_run_isNoop_ifDisabled()
  {
    killerConfig = new UnusedSegmentKillerConfig(false, null, null, null);
    initKiller();

    Assertions.assertFalse(killer.isEnabled());

    killer.run();
    Assertions.assertFalse(killExecutor.hasPendingTasks());
  }

  @Test
  public void test_run_doesNotProcessSegments_ifNotLeader()
  {
    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    leaderSelector.becomeLeader();
    killer.run();

    leaderSelector.stopBeingLeader();

    Assertions.assertTrue(killExecutor.hasPendingTasks());

    finishQueuedKillJobs();
    emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS);
    Assertions.assertFalse(killExecutor.hasPendingTasks());
  }

  @Test
  public void test_run_launchesEmbeddedKillTasks_ifLeader()
  {
    leaderSelector.becomeLeader();

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Reset the queue and verify that kill jobs have been added to the queue
    killer.run();
    Assertions.assertTrue(killExecutor.hasPendingTasks());
    emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS);

    finishQueuedKillJobs();
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS, 10);

    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.QUEUE_PROCESS_TIME, 1);
    emitter.verifyEmitted(TaskMetrics.RUN_DURATION, 10);

    emitter.verifyEmitted(TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE, 10);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE, 10L);

    emitter.verifyEmitted(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10L);

    Assertions.assertTrue(
        retrieveUnusedSegments(Intervals.ETERNITY).isEmpty()
    );
  }

  @Test
  public void test_maxSegmentsKilledInRun_isLimitedByConfig()
  {
    killerConfig = new UnusedSegmentKillerConfig(true, zeroBufferPeriod(), null, 700);
    initKiller();
    leaderSelector.becomeLeader();

    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(10, Granularities.DAY)
                          .withNumPartitions(100)
                          .eachOfSizeInMb(50);

    storageCoordinator.commitSegments(Set.copyOf(segments), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    Assertions.assertEquals(
        1000,
        retrieveUnusedSegments(Intervals.ETERNITY).size()
    );

    resetKillQueue();
    finishQueuedKillJobs();

    // Verify that a total of 700 segments were identified for kill
    emitter.verifySum(UnusedSegmentsKiller.Metric.ELIGIBLE_UNUSED_SEGMENTS, 700L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 700L);
  }

  @Timeout(20)
  @Test
  public void test_maxSegmentsKilledByTask_is_10k()
  {
    leaderSelector.becomeLeader();

    // Create an interval with 12k killable unused segments
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(1, Granularities.DAY)
                          .withNumPartitions(12_000)
                          .eachOfSizeInMb(50);

    storageCoordinator.commitSegments(Set.copyOf(segments), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    Assertions.assertEquals(
        12_000,
        retrieveUnusedSegments(Intervals.ETERNITY).size()
    );

    resetKillQueue();
    finishQueuedKillJobs();

    // Verify that all the segments in the interval were eligible for kill
    emitter.verifySum(UnusedSegmentsKiller.Metric.UNUSED_SEGMENT_INTERVALS, 1L);
    emitter.verifySum(UnusedSegmentsKiller.Metric.ELIGIBLE_UNUSED_SEGMENTS, 12_000L);

    // Verify that 2 tasks were launched to kill the segments
    emitter.verifySum(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS, 2L);
    final List<String> taskIds = emitter.getMetricEvents(TaskMetrics.RUN_DURATION)
                                        .stream()
                                        .map(event -> event.getUserDims().get("taskId").toString())
                                        .toList();
    Assertions.assertEquals(2, taskIds.size());

    // Verify that the tasks killed 10k and 2k segments respectively
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, Map.of("taskId", taskIds.get(0)), 10_000);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, Map.of("taskId", taskIds.get(1)), 2_000);
  }

  @Timeout(20)
  @Test
  public void test_maxIntervalsKilledInADatasource_is_10k()
  {
    leaderSelector.becomeLeader();

    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(10_001, Granularities.DAY)
                          .eachOfSizeInMb(50);

    storageCoordinator.commitSegments(Set.copyOf(segments), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    Assertions.assertEquals(
        10_001,
        retrieveUnusedSegments(Intervals.ETERNITY).size()
    );

    resetKillQueue();

    emitter.verifySum(UnusedSegmentsKiller.Metric.UNUSED_SEGMENT_INTERVALS, 10_000L);
    emitter.verifySum(UnusedSegmentsKiller.Metric.ELIGIBLE_UNUSED_SEGMENTS, 10_000L);
  }

  @Test
  public void test_run_resetsQueue_ifLeadershipIsReacquired()
  {
    leaderSelector.becomeLeader();

    // Verify that the queue has been reset
    killer.run();
    finishQueuedKillJobs();
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.QUEUE_RESET_TIME, 1);
    emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS);

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Lose and reacquire leadership
    leaderSelector.stopBeingLeader();
    leaderSelector.becomeLeader();

    // Run again and verify that queue has been reset
    emitter.flush();
    killer.run();
    finishQueuedKillJobs();
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.QUEUE_RESET_TIME, 1);
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS, 10);
  }

  @Test
  public void test_run_doesNotResetQueue_ifThereArePendingJobs_andLastRunWasLessThanOneDayAgo()
  {
    leaderSelector.becomeLeader();

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Invoke run, reset the queue and process only some of the jobs
    killer.run();
    killExecutor.finishNextPendingTasks(6);
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.QUEUE_RESET_TIME, 1);
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS, 5);

    Assertions.assertTrue(killExecutor.hasPendingTasks());

    // Invoke run again and verify that queue has not been reset
    emitter.flush();
    killer.run();
    finishQueuedKillJobs();
    emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.QUEUE_RESET_TIME);
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS, 5);

    // All jobs have been processed
    Assertions.assertFalse(killExecutor.hasPendingTasks());
  }

  @Test
  public void test_run_prioritizesOlderIntervals()
  {
    leaderSelector.becomeLeader();

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    killer.run();
    finishQueuedKillJobs();
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS, 10);

    // Verify that the kill intervals are sorted with the oldest interval first
    final List<ServiceMetricEvent> events = emitter.getMetricEvents(TaskMetrics.RUN_DURATION);
    final List<Interval> killIntervals = events.stream().map(event -> {
      final String taskId = (String) event.getUserDims().get(DruidMetrics.TASK_ID);
      String[] splits = taskId.split("_");
      return Intervals.of(splits[4] + "/" + splits[5]);
    }).collect(Collectors.toList());

    Assertions.assertEquals(10, killIntervals.size());

    final List<Interval> expectedIntervals =
        WIKI_SEGMENTS_1X10D.stream()
                           .map(DataSegment::getInterval)
                           .sorted(Comparators.intervalsByEndThenStart())
                           .collect(Collectors.toList());
    Assertions.assertEquals(expectedIntervals, killIntervals);
  }

  @Test
  public void test_run_doesNotDeleteSegmentFiles_ifLoadSpecIsUsedByAnotherSegment()
  {
    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Add a new segment upgraded from one of the unused segments
    final DataSegment upgradedSegment1 = WIKI_SEGMENTS_1X10D.get(0).withVersion("v2");
    final DataSegment upgradedSegment2 = WIKI_SEGMENTS_1X10D.get(1).withVersion("v2");
    storageCoordinator.commitSegments(Set.of(upgradedSegment1, upgradedSegment2), null);

    leaderSelector.becomeLeader();
    killer.run();

    // Verify that all unused segments are deleted from metadata store but the
    // ones with used load specs are not deleted from the deep store
    finishQueuedKillJobs();
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE, 10L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 8L);
  }

  @Test
  public void test_run_isNoop_ifRetrieveUpgradedToSegmentIdsFails()
  {
    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Make the retrieveUpgradedFromSegmentIds task action fail
    taskActionTestKit.registerDelegateForTaskAction(
        RetrieveUpgradedToSegmentIdsAction.class,
        () -> {
          throw new ISE("Failed to fetch children IDs");
        }
    );

    leaderSelector.becomeLeader();
    killer.run();

    // Verify that no unused segment is deleted from metadata store or deep store
    finishQueuedKillJobs();
    emitter.verifyNotEmitted(TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE);
    emitter.verifyNotEmitted(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE);

    // Verify that the task is marked as failed
    emitter.verifyEmitted("task/run/time", Map.of("taskStatus", "FAILED"), 10);
  }

  @Test
  public void test_run_doesNotKillSegment_ifUpdatedWithinBufferPeriod()
  {
    killerConfig = new UnusedSegmentKillerConfig(true, Period.hours(1), null, null);
    initKiller();

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    leaderSelector.becomeLeader();
    killer.run();
    finishQueuedKillJobs();

    // Verify that no tasks are launched
    emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.UNUSED_SEGMENT_INTERVALS);
    emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.ELIGIBLE_UNUSED_SEGMENTS);

    emitter.verifyNotEmitted(TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE);
    emitter.verifyNotEmitted(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE);
  }

  @Test
  public void test_run_killsFutureSegment()
  {
    final List<DataSegment> futureSegments = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .forIntervals(10, Granularities.DAY)
        .startingAt("2050-01-01")
        .eachOfSize(500);
    storageCoordinator.commitSegments(Set.copyOf(futureSegments), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    leaderSelector.becomeLeader();
    killer.run();
    finishQueuedKillJobs();

    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE, 10L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10L);
  }

  @Test
  public void test_run_doesNotSkipLockedIntervals() throws InterruptedException
  {
    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_1X10D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Lock up some of the segment intervals
    final Interval lockedInterval = new Interval(
        WIKI_SEGMENTS_1X10D.get(0).getInterval().getStart(),
        WIKI_SEGMENTS_1X10D.get(4).getInterval().getEnd()
    );

    final Task ingestionTask = new NoopTask(null, null, TestDataSource.WIKI, 0L, 0L, null);
    final GlobalTaskLockbox taskLockbox = taskActionTestKit.getTaskLockbox();

    try {
      taskLockbox.add(ingestionTask);
      taskLockbox.lock(
          ingestionTask,
          new TimeChunkLockRequest(TaskLockType.APPEND, ingestionTask, lockedInterval, null)
      );

      leaderSelector.becomeLeader();
      killer.run();
      finishQueuedKillJobs();

      // Verify that unused segments from locked intervals are also killed
      emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE, 10L);
      emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10L);
      emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.SKIPPED_INTERVALS);
    }
    finally {
      taskLockbox.remove(ingestionTask);
    }

    // Do another run to clean up the rest of the segments
    killer.run();
    finishQueuedKillJobs();
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_METADATA_STORE, 10L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10L);
  }

  private List<DataSegment> retrieveUnusedSegments(Interval interval)
  {
    return storageCoordinator.retrieveUnusedSegmentsForInterval(
        TestDataSource.WIKI,
        interval,
        null,
        null
    );
  }

  /**
   * Buffer period which ensures that segments are killed as soon as they become unused.
   */
  private static Period zeroBufferPeriod()
  {
    // Subtract the grace period
    return Period.ZERO.minus(UnusedSegmentKillerConfig.GRACE_PERIOD);
  }
}
