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
import org.apache.druid.indexing.common.actions.LocalTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionTestKit;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskMetrics;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.UnusedSegmentKillerConfig;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestDruidLeaderSelector;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class UnusedSegmentsKillerTest
{
  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private static final List<DataSegment> WIKI_SEGMENTS_10X1D =
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

  @Before
  public void setup()
  {
    emitter = taskActionTestKit.getServiceEmitter();
    leaderSelector = new TestDruidLeaderSelector();
    dataSegmentKiller = new TestDataSegmentKiller();
    killerConfig = new UnusedSegmentKillerConfig(true, Period.ZERO);
    killExecutor = new BlockingExecutorService("UnusedSegmentsKillerTest-%s");
    storageCoordinator = taskActionTestKit.getMetadataStorageCoordinator();
    initKiller();
  }

  private void initKiller()
  {
    killer = new UnusedSegmentsKiller(
        new SegmentsMetadataManagerConfig(
            null,
            SegmentMetadataCache.UsageMode.ALWAYS,
            killerConfig
        ),
        task -> new LocalTaskActionClient(task, taskActionTestKit.getTaskActionToolbox()),
        storageCoordinator,
        leaderSelector,
        (corePoolSize, nameFormat) -> new WrappingScheduledExecutorService(nameFormat, killExecutor, true),
        dataSegmentKiller,
        taskActionTestKit.getTaskLockbox(),
        taskActionTestKit.getServiceEmitter()
    );
  }

  private void finishQueuedKillJobs()
  {
    killExecutor.finishAllPendingTasks();
  }

  @Test
  public void test_getSchedule_returnsOneHourPeriod_ifEnabled()
  {
    final DutySchedule schedule = killer.getSchedule();
    Assert.assertEquals(Duration.standardHours(1).getMillis(), schedule.getPeriodMillis());
    Assert.assertEquals(Duration.standardMinutes(1).getMillis(), schedule.getInitialDelayMillis());
  }

  @Test
  public void test_getSchedule_returnsZeroPeriod_ifDisabled()
  {
    killerConfig = new UnusedSegmentKillerConfig(false, null);
    initKiller();

    final DutySchedule schedule = killer.getSchedule();
    Assert.assertEquals(0, schedule.getPeriodMillis());
    Assert.assertEquals(0, schedule.getInitialDelayMillis());
  }

  @Test
  public void test_run_startsProcessing_ifEnabled()
  {
    Assert.assertFalse(killExecutor.hasPendingTasks());
    Assert.assertTrue(killer.isEnabled());

    killer.run();
    Assert.assertTrue(killExecutor.hasPendingTasks());
  }

  @Test
  public void test_run_isNoop_ifDisabled()
  {
    killerConfig = new UnusedSegmentKillerConfig(false, null);
    initKiller();

    Assert.assertFalse(killer.isEnabled());

    killer.run();
    Assert.assertFalse(killExecutor.hasPendingTasks());
  }

  @Test
  public void test_run_doesNotProcessSegments_ifNotLeader()
  {
    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_10X1D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    leaderSelector.becomeLeader();
    killer.run();

    leaderSelector.stopBeingLeader();

    Assert.assertTrue(killExecutor.hasPendingTasks());

    finishQueuedKillJobs();
    emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS);
    Assert.assertFalse(killExecutor.hasPendingTasks());
  }

  @Test
  public void test_run_launchesEmbeddedKillTasks_ifLeader()
  {
    leaderSelector.becomeLeader();

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_10X1D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    killer.run();

    // Verify that the queue still has pending jobs
    Assert.assertTrue(killExecutor.hasPendingTasks());
    emitter.verifyNotEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS);

    finishQueuedKillJobs();
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS, 10);

    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.QUEUE_PROCESS_TIME, 1);
    emitter.verifyEmitted(TaskMetrics.RUN_DURATION, 10);

    emitter.verifyEmitted(TaskMetrics.NUKED_SEGMENTS, 10);
    emitter.verifySum(TaskMetrics.NUKED_SEGMENTS, 10L);

    emitter.verifyEmitted(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10L);
  }

  @Test
  public void test_run_resetsQueue_ifLeadershipIsReacquired()
  {
    leaderSelector.becomeLeader();

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_10X1D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Queue the unused segments for kill
    killer.run();

    // TODO: finish this test
  }

  @Test
  public void test_run_doesNotResetQueue_ifLastRunWasLessThanOneDayAgo()
  {

  }

  @Test
  public void test_run_prioritizesOlderIntervals()
  {

  }

  @Test
  public void test_run_doesNotDeleteSegmentFiles_ifLoadSpecIsUsedByAnotherSegment()
  {
    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_10X1D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Add a new segment upgraded from one of the unused segments
    final DataSegment upgradedSegment1 = WIKI_SEGMENTS_10X1D.get(0).withVersion("v2");
    final DataSegment upgradedSegment2 = WIKI_SEGMENTS_10X1D.get(1).withVersion("v2");
    storageCoordinator.commitSegments(Set.of(upgradedSegment1, upgradedSegment2), null);

    leaderSelector.becomeLeader();
    killer.run();

    // Verify that all unused segments are deleted from metadata store but the
    // ones with used load specs are not deleted from the deep store
    finishQueuedKillJobs();
    emitter.verifySum(TaskMetrics.NUKED_SEGMENTS, 10L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 8L);
  }

  @Test
  public void test_run_withMultipleDatasources()
  {

  }

  @Test
  public void test_run_doesNotKillSegment_ifUpdatedWithinBufferPeriod()
  {
    killerConfig = new UnusedSegmentKillerConfig(true, Period.hours(1));
    initKiller();

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_10X1D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    leaderSelector.becomeLeader();
    killer.run();
    finishQueuedKillJobs();

    // Verify that tasks are launched but no segment is killed
    emitter.verifyValue(UnusedSegmentsKiller.Metric.UNUSED_SEGMENT_INTERVALS, 10L);
    emitter.verifyEmitted(UnusedSegmentsKiller.Metric.PROCESSED_KILL_JOBS, 10);
    emitter.verifyEmitted(TaskMetrics.RUN_DURATION, 10);

    emitter.verifySum(TaskMetrics.NUKED_SEGMENTS, 0L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 0L);
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

    emitter.verifySum(TaskMetrics.NUKED_SEGMENTS, 10L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10L);
  }

  @Test
  public void test_run_skipsLockedIntervals() throws InterruptedException
  {
    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_10X1D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    // Lock up some of the segment intervals
    final Interval lockedInterval = new Interval(
        WIKI_SEGMENTS_10X1D.get(0).getInterval().getStart(),
        WIKI_SEGMENTS_10X1D.get(4).getInterval().getEnd()
    );

    final Task ingestionTask = new NoopTask(null, null, TestDataSource.WIKI, 0L, 0L, null);
    final TaskLockbox taskLockbox = taskActionTestKit.getTaskLockbox();

    try {
      taskLockbox.add(ingestionTask);
      taskLockbox.lock(
          ingestionTask,
          new TimeChunkLockRequest(TaskLockType.APPEND, ingestionTask, lockedInterval, null)
      );

      leaderSelector.becomeLeader();
      killer.run();
      finishQueuedKillJobs();

      // Verify that unused segments from locked intervals are not killed
      emitter.verifySum(TaskMetrics.NUKED_SEGMENTS, 5L);
      emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 5L);
      emitter.verifySum(UnusedSegmentsKiller.Metric.SKIPPED_INTERVALS, 5L);
    }
    finally {
      taskLockbox.remove(ingestionTask);
    }

    // Do another run to clean up the rest of the segments
    killer.run();
    finishQueuedKillJobs();
    emitter.verifySum(TaskMetrics.NUKED_SEGMENTS, 10L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10L);
  }

  @Test
  public void test_run_killsSegmentUpdatedInFuture_ifBufferPeriodIsNegative()
  {
    killerConfig = new UnusedSegmentKillerConfig(true, Period.days(1).negated());
    initKiller();

    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_10X1D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    setLastUpdatedTime(DateTimes.nowUtc().plusHours(10));

    leaderSelector.becomeLeader();
    killer.run();
    finishQueuedKillJobs();

    emitter.verifySum(TaskMetrics.NUKED_SEGMENTS, 10L);
    emitter.verifySum(TaskMetrics.SEGMENTS_DELETED_FROM_DEEPSTORE, 10L);
  }

  private void setLastUpdatedTime(DateTime lastUpdatedTime)
  {
    final String sql = StringUtils.format(
        "UPDATE %1$s SET used_status_last_updated = ?",
        taskActionTestKit.getMetadataStorageTablesConfig().getSegmentsTable()
    );
    taskActionTestKit.getTestDerbyConnector().retryWithHandle(
        handle -> handle.update(sql, lastUpdatedTime.toString())
    );
  }
}
