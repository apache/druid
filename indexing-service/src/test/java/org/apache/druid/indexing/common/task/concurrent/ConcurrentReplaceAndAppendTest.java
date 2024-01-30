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

package org.apache.druid.indexing.common.task.concurrent;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.common.MultipleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskStorageDirTracker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.IngestionTestBase;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TestTaskToolboxFactory;
import org.apache.druid.indexing.overlord.ThreadingTaskRunner;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contains tests to verify behaviour of concurrently running REPLACE and APPEND
 * tasks on the same interval of a datasource.
 * <p>
 * The tests verify the interleaving of the following actions:
 * <ul>
 *   <li>LOCK: Acquisition of a lock on an interval by a replace task</li>
 *   <li>ALLOCATE: Allocation of a pending segment by an append task</li>
 *   <li>REPLACE: Commit of segments created by a replace task</li>
 *   <li>APPEND: Commit of segments created by an append task</li>
 * </ul>
 */
public class ConcurrentReplaceAndAppendTest extends IngestionTestBase
{
  /**
   * The version used by append jobs when no previous replace job has run on an interval.
   */
  private static final String SEGMENT_V0 = DateTimes.EPOCH.toString();

  private static final Interval YEAR_23 = Intervals.of("2023/2024");
  private static final Interval JAN_23 = Intervals.of("2023-01/2023-02");
  private static final Interval DEC_23 = Intervals.of("2023-12/2024-01");
  private static final Interval OCT_NOV_DEC_23 = Intervals.of("2023-10-01/2024-01-01");
  private static final Interval FIRST_OF_JAN_23 = Intervals.of("2023-01-01/2023-01-02");

  private static final String WIKI = "wiki";

  private TaskQueue taskQueue;
  private TaskActionClientFactory taskActionClientFactory;
  private TaskActionClient dummyTaskActionClient;
  private final List<ActionsTestTask> runningTasks = new ArrayList<>();

  private ActionsTestTask appendTask;
  private ActionsTestTask replaceTask;

  private final AtomicInteger groupId = new AtomicInteger(0);

  @Before
  public void setup()
  {
    final TaskConfig taskConfig = new TaskConfigBuilder().build();
    taskActionClientFactory = createActionClientFactory();
    dummyTaskActionClient = taskActionClientFactory.create(NoopTask.create());

    final WorkerConfig workerConfig = new WorkerConfig().setCapacity(10);
    TaskRunner taskRunner = new ThreadingTaskRunner(
        createToolboxFactory(taskConfig, taskActionClientFactory),
        taskConfig,
        workerConfig,
        new NoopTaskLogs(),
        getObjectMapper(),
        new TestAppenderatorsManager(),
        new MultipleFileTaskReportFileWriter(),
        new DruidNode("middleManager", "host", false, 8091, null, true, false),
        TaskStorageDirTracker.fromConfigs(workerConfig, taskConfig)
    );
    taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, new Period(0L), null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        taskRunner,
        taskActionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    runningTasks.clear();
    taskQueue.start();

    groupId.set(0);
    appendTask = createAndStartTask();
    replaceTask = createAndStartTask();
  }

  @After
  public void tearDown()
  {
    for (ActionsTestTask task : runningTasks) {
      task.finishRunAndGetStatus();
    }
  }

  @Test
  public void testLockReplaceAllocateAppend()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);

    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(segmentV10.getVersion(), pendingSegment.getVersion());

    final DataSegment segmentV11 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV11);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testLockAllocateAppendDayReplaceDay()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    // Verify that the segment appended to v0 gets upgraded to v1
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .version(v1).build();
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testLockAllocateReplaceDayAppendDay()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    // Verify that the segment appended to v0 gets upgraded to v1
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .version(v1).build();
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testAllocateLockReplaceDayAppendDay()
  {
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    // Verify that the segment appended to v0 gets upgraded to v1
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .version(v1).build();
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testAllocateLockAppendDayReplaceDay()
  {
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    replaceTask.finishRunAndGetStatus();

    // Verify that the segment appended to v0 gets upgraded to v1
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .version(v1).build();
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testAllocateAppendDayLockReplaceDay()
  {
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    // Verify that the segment appended to v0 gets fully overshadowed
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);
  }

  @Test
  public void testLockReplaceMonthAllocateAppendDay()
  {
    String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    // Verify that the allocated segment takes the version and interval of previous replace
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(v1, pendingSegment.getVersion());

    final DataSegment segmentV11 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV11);

    verifyIntervalHasUsedSegments(JAN_23, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testLockAllocateAppendDayReplaceMonth()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    // Verify that append segment gets upgraded to replace version
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .version(v1)
                                              .interval(segmentV10.getInterval())
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .build();
    verifyIntervalHasUsedSegments(JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testLockAllocateReplaceMonthAppendDay()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    // Verify that append segment gets upgraded to replace version
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .version(v1)
                                              .interval(segmentV10.getInterval())
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .build();
    verifyIntervalHasUsedSegments(JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testAllocateLockReplaceMonthAppendDay()
  {
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    // Verify that append segment gets upgraded to replace version
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .version(v1)
                                              .interval(segmentV10.getInterval())
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .build();
    verifyIntervalHasUsedSegments(JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testAllocateLockAppendDayReplaceMonth()
  {
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    // Verify that append segment gets upgraded to replace version
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .version(v1)
                                              .interval(segmentV10.getInterval())
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .build();
    verifyIntervalHasUsedSegments(JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testAllocateAppendDayLockReplaceMonth()
  {
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    // Verify that the old segment gets completely replaced
    verifyIntervalHasUsedSegments(JAN_23, segmentV01, segmentV10);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10);
  }

  @Test
  public void testLockReplaceDayAllocateAppendMonth()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    // Verify that an APPEND lock cannot be acquired on month
    TaskLock appendLock = appendTask.acquireAppendLockOn(JAN_23);
    Assert.assertNull(appendLock);

    // Verify that new segment gets allocated with DAY granularity even though preferred was MONTH
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertEquals(v1, pendingSegment.getVersion());
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment.getInterval());

    final DataSegment segmentV11 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV11);

    verifyIntervalHasUsedSegments(JAN_23, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testLockAllocateAppendMonthReplaceDay()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    // Verify that an APPEND lock cannot be acquired on month
    TaskLock appendLock = appendTask.acquireAppendLockOn(JAN_23);
    Assert.assertNull(appendLock);

    // Verify that the segment is allocated for DAY granularity
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    // Verify that append segment gets upgraded to replace version
    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .version(v1)
                                              .interval(segmentV10.getInterval())
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .build();
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testLockAllocateReplaceDayAppendMonth()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    // Verify that an APPEND lock cannot be acquired on month
    TaskLock appendLock = appendTask.acquireAppendLockOn(JAN_23);
    Assert.assertNull(appendLock);

    // Verify that the segment is allocated for DAY granularity instead of MONTH
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .interval(FIRST_OF_JAN_23)
                                              .version(v1)
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .build();

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testAllocateLockReplaceDayAppendMonth()
  {
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertEquals(JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    // Verify that replace lock cannot be acquired on MONTH
    TaskLock replaceLock = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23);
    Assert.assertNull(replaceLock);

    // Verify that segment cannot be committed since there is no lock
    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, SEGMENT_V0);
    final ISE exception = Assert.assertThrows(ISE.class, () -> replaceTask.commitReplaceSegments(segmentV10));
    final Throwable throwable = Throwables.getRootCause(exception);
    Assert.assertEquals(
        StringUtils.format(
            "Segments[[%s]] are not covered by locks[[]] for task[%s]",
            segmentV10, replaceTask.getId()
        ),
        throwable.getMessage()
    );

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);
    verifyIntervalHasUsedSegments(JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV01);
  }

  @Test
  public void testAllocateAppendMonthLockReplaceDay()
  {
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertEquals(JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV01);

    // Verify that replace lock cannot be acquired on DAY as MONTH is already locked
    final TaskLock replaceLock = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23);
    Assert.assertNull(replaceLock);
  }

  @Test
  public void testLockReplaceAllocateLockReplaceLockReplaceAppend()
  {
    // Commit initial segments for v1
    final ActionsTestTask replaceTask1 = createAndStartTask();
    final String v1 = replaceTask1.acquireReplaceLockOn(YEAR_23).getVersion();

    final DataSegment segmentV10 = createSegment(YEAR_23, v1);
    replaceTask1.commitReplaceSegments(segmentV10);
    replaceTask1.finishRunAndGetStatus();
    verifyIntervalHasUsedSegments(YEAR_23, segmentV10);
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV10);

    // Allocate an append segment for v1
    final ActionsTestTask appendTask1 = createAndStartTask();
    final SegmentIdWithShardSpec pendingSegmentV11
        = appendTask1.allocateSegmentForTimestamp(YEAR_23.getStart(), Granularities.YEAR);
    Assert.assertEquals(v1, pendingSegmentV11.getVersion());
    Assert.assertEquals(YEAR_23, pendingSegmentV11.getInterval());

    // Commit replace segment for v2
    final ActionsTestTask replaceTask2 = createAndStartTask();
    final String v2 = replaceTask2.acquireReplaceLockOn(YEAR_23).getVersion();

    final DataSegment segmentV20 = DataSegment.builder(segmentV10).version(v2).build();
    replaceTask2.commitReplaceSegments(segmentV20);
    replaceTask2.finishRunAndGetStatus();
    verifyIntervalHasUsedSegments(YEAR_23, segmentV10, segmentV20);
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV20);

    final ActionsTestTask replaceTask3 = createAndStartTask();
    final String v3 = replaceTask3.acquireReplaceLockOn(YEAR_23).getVersion();

    // Commit append segment to v1 and verify that it gets upgraded to v2
    final DataSegment segmentV11 = asSegment(pendingSegmentV11);
    final DataSegment segmentV21 = DataSegment.builder(segmentV11).version(v2).build();
    Set<DataSegment> appendedSegments = appendTask1.commitAppendSegments(segmentV11).getSegments();
    Assert.assertEquals(Sets.newHashSet(segmentV21, segmentV11), appendedSegments);

    appendTask1.finishRunAndGetStatus();
    verifyIntervalHasUsedSegments(
        YEAR_23,
        segmentV20, segmentV21, segmentV10, segmentV11
    );
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV20, segmentV21);

    // Commit replace segment v2 and verify that append segment gets upgraded to v2
    final DataSegment segmentV30 = DataSegment.builder(segmentV20).version(v3).build();
    replaceTask3.commitReplaceSegments(segmentV30);
    replaceTask3.finishRunAndGetStatus();

    final DataSegment segmentV31 = DataSegment.builder(segmentV21).version(v3).build();
    verifyIntervalHasUsedSegments(
        YEAR_23,
        segmentV10, segmentV11, segmentV20, segmentV21, segmentV30, segmentV31
    );
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV30, segmentV31);
  }

  @Test
  public void testLockReplaceMultipleAppends()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);

    appendTask.acquireAppendLockOn(FIRST_OF_JAN_23);
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(segmentV10.getVersion(), pendingSegment.getVersion());

    final ActionsTestTask appendTask2 = createAndStartTask();
    appendTask2.acquireAppendLockOn(FIRST_OF_JAN_23);
    final SegmentIdWithShardSpec pendingSegment2
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(segmentV10.getVersion(), pendingSegment2.getVersion());

    final DataSegment segmentV11 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV11);

    final DataSegment segmentV12 = asSegment(pendingSegment2);
    appendTask.commitAppendSegments(segmentV12);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10, segmentV11, segmentV12);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11, segmentV12);
  }

  @Test
  public void testMultipleGranularities()
  {
    // Allocate segment for Jan 1st
    appendTask.acquireAppendLockOn(FIRST_OF_JAN_23);
    final SegmentIdWithShardSpec pendingSegment01
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment01.getVersion());
    Assert.assertEquals(FIRST_OF_JAN_23, pendingSegment01.getInterval());

    // Allocate segment for Oct-Dec
    final ActionsTestTask appendTask2 = createAndStartTask();
    appendTask2.acquireAppendLockOn(OCT_NOV_DEC_23);
    final SegmentIdWithShardSpec pendingSegment02
        = appendTask2.allocateSegmentForTimestamp(OCT_NOV_DEC_23.getStart(), Granularities.QUARTER);
    Assert.assertEquals(SEGMENT_V0, pendingSegment02.getVersion());
    Assert.assertEquals(OCT_NOV_DEC_23, pendingSegment02.getInterval());

    // Append segment for Oct-Dec
    final DataSegment segmentV02 = asSegment(pendingSegment02);
    appendTask2.commitAppendSegments(segmentV02);
    verifyIntervalHasUsedSegments(YEAR_23, segmentV02);
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV02);

    // Try to Allocate segment for Dec
    final ActionsTestTask appendTask3 = createAndStartTask();
    appendTask3.acquireAppendLockOn(DEC_23);
    final SegmentIdWithShardSpec pendingSegment03
        = appendTask3.allocateSegmentForTimestamp(DEC_23.getStart(), Granularities.MONTH);

    // Verify that segment gets allocated for quarter instead of month
    Assert.assertEquals(SEGMENT_V0, pendingSegment03.getVersion());
    Assert.assertEquals(OCT_NOV_DEC_23, pendingSegment03.getInterval());

    // Acquire replace lock on whole year
    final String v1 = replaceTask.acquireReplaceLockOn(YEAR_23).getVersion();

    // Append segment for Jan 1st
    final DataSegment segmentV01 = asSegment(pendingSegment01);
    appendTask.commitAppendSegments(segmentV01);
    verifyIntervalHasUsedSegments(YEAR_23, segmentV01, segmentV02);
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV01, segmentV02);

    // Replace segment for whole year
    final DataSegment segmentV10 = createSegment(YEAR_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);

    final DataSegment segmentV11 = DataSegment.builder(segmentV01)
                                              .version(v1)
                                              .interval(YEAR_23)
                                              .shardSpec(new NumberedShardSpec(1, 1))
                                              .build();

    // Verify that segmentV01 is upgraded to segmentV11 and segmentV02 is replaced
    verifyIntervalHasUsedSegments(YEAR_23, segmentV01, segmentV02, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV10, segmentV11);

    // Append segment for quarter
    final DataSegment segmentV03 = asSegment(pendingSegment03);
    appendTask3.commitAppendSegments(segmentV03);

    final DataSegment segmentV13 = DataSegment.builder(segmentV03)
                                              .version(v1)
                                              .interval(YEAR_23)
                                              .shardSpec(new NumberedShardSpec(2, 1))
                                              .build();

    verifyIntervalHasUsedSegments(YEAR_23, segmentV01, segmentV02, segmentV03, segmentV10, segmentV11, segmentV13);
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV10, segmentV11, segmentV13);
  }

  @Test
  public void testSegmentIsAllocatedAtLatestVersion()
  {
    final SegmentIdWithShardSpec pendingSegmentV01
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertEquals(SEGMENT_V0, pendingSegmentV01.getVersion());
    Assert.assertEquals(JAN_23, pendingSegmentV01.getInterval());

    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();
    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10);

    final SegmentIdWithShardSpec pendingSegmentV12
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertNotEquals(pendingSegmentV01.asSegmentId(), pendingSegmentV12.asSegmentId());
    Assert.assertEquals(v1, pendingSegmentV12.getVersion());
    Assert.assertEquals(JAN_23, pendingSegmentV12.getInterval());

    replaceTask.releaseLock(JAN_23);
    final ActionsTestTask replaceTask2 = createAndStartTask();
    final String v2 = replaceTask2.acquireReplaceLockOn(JAN_23).getVersion();
    final DataSegment segmentV20 = createSegment(JAN_23, v2);
    replaceTask2.commitReplaceSegments(segmentV20);
    verifyIntervalHasUsedSegments(JAN_23, segmentV10, segmentV20);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV20);

    final SegmentIdWithShardSpec pendingSegmentV23
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertNotEquals(pendingSegmentV01.asSegmentId(), pendingSegmentV23.asSegmentId());
    Assert.assertEquals(v2, pendingSegmentV23.getVersion());
    Assert.assertEquals(JAN_23, pendingSegmentV23.getInterval());

    // Commit the append segments
    final DataSegment segmentV01 = asSegment(pendingSegmentV01);
    final DataSegment segmentV12 = asSegment(pendingSegmentV12);
    final DataSegment segmentV23 = asSegment(pendingSegmentV23);

    Set<DataSegment> appendedSegments
        = appendTask.commitAppendSegments(segmentV01, segmentV12, segmentV23).getSegments();
    Assert.assertEquals(3 + 3, appendedSegments.size());

    // Verify that the original append segments have been committed
    Assert.assertTrue(appendedSegments.remove(segmentV01));
    Assert.assertTrue(appendedSegments.remove(segmentV12));
    Assert.assertTrue(appendedSegments.remove(segmentV23));

    // Verify that segmentV01 has been upgraded to both v1 and v2
    final DataSegment segmentV11 = findSegmentWith(v1, segmentV01.getLoadSpec(), appendedSegments);
    Assert.assertNotNull(segmentV11);
    final DataSegment segmentV21 = findSegmentWith(v2, segmentV01.getLoadSpec(), appendedSegments);
    Assert.assertNotNull(segmentV21);

    // Verify that segmentV12 has been upgraded to v2
    final DataSegment segmentV22 = findSegmentWith(v2, segmentV12.getLoadSpec(), appendedSegments);
    Assert.assertNotNull(segmentV22);

    // Verify that segmentV23 is not downgraded to v1
    final DataSegment segmentV13 = findSegmentWith(v1, segmentV23.getLoadSpec(), appendedSegments);
    Assert.assertNull(segmentV13);

    verifyIntervalHasUsedSegments(
        YEAR_23,
        segmentV01,
        segmentV10, segmentV11, segmentV12,
        segmentV20, segmentV21, segmentV22, segmentV23
    );
    verifyIntervalHasVisibleSegments(YEAR_23, segmentV20, segmentV21, segmentV22, segmentV23);
  }

  @Test
  public void testSegmentsToReplace()
  {
    final SegmentIdWithShardSpec pendingSegmentV01
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertEquals(SEGMENT_V0, pendingSegmentV01.getVersion());
    Assert.assertEquals(JAN_23, pendingSegmentV01.getInterval());
    final DataSegment segment1 = asSegment(pendingSegmentV01);
    appendTask.commitAppendSegments(segment1);

    final SegmentIdWithShardSpec pendingSegmentV02
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertNotEquals(pendingSegmentV01.asSegmentId(), pendingSegmentV02.asSegmentId());
    Assert.assertEquals(SEGMENT_V0, pendingSegmentV02.getVersion());
    Assert.assertEquals(JAN_23, pendingSegmentV02.getInterval());

    verifyInputSegments(replaceTask, JAN_23, segment1);

    replaceTask.acquireReplaceLockOn(JAN_23);

    final DataSegment segment2 = asSegment(pendingSegmentV02);
    appendTask.commitAppendSegments(segment2);

    // Despite segment2 existing, it is not chosen to be replaced because it was created after the tasklock was acquired
    verifyInputSegments(replaceTask, JAN_23, segment1);

    replaceTask.releaseLock(JAN_23);

    final SegmentIdWithShardSpec pendingSegmentV03
        = appendTask.allocateSegmentForTimestamp(JAN_23.getStart(), Granularities.MONTH);
    Assert.assertNotEquals(pendingSegmentV01.asSegmentId(), pendingSegmentV03.asSegmentId());
    Assert.assertNotEquals(pendingSegmentV02.asSegmentId(), pendingSegmentV03.asSegmentId());
    Assert.assertEquals(SEGMENT_V0, pendingSegmentV03.getVersion());
    Assert.assertEquals(JAN_23, pendingSegmentV03.getInterval());
    final DataSegment segment3 = asSegment(pendingSegmentV03);
    appendTask.commitAppendSegments(segment3);
    appendTask.releaseLock(JAN_23);

    replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23);
    // The new lock was acquired before segment3 was created but it doesn't contain the month's interval
    // So, all three segments are chosen
    verifyInputSegments(replaceTask, JAN_23, segment1, segment2, segment3);

    replaceTask.releaseLock(FIRST_OF_JAN_23);
    // All the segments are chosen when there's no lock
    verifyInputSegments(replaceTask, JAN_23, segment1, segment2, segment3);
  }

  @Test
  public void testLockAllocateDayReplaceMonthAllocateAppend()
  {
    final SegmentIdWithShardSpec pendingSegmentV0
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);

    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(JAN_23, segmentV10);

    final SegmentIdWithShardSpec pendingSegmentV1
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(segmentV10.getVersion(), pendingSegmentV1.getVersion());

    final DataSegment segmentV00 = asSegment(pendingSegmentV0);
    final DataSegment segmentV11 = asSegment(pendingSegmentV1);
    Set<DataSegment> appendSegments = appendTask.commitAppendSegments(segmentV00, segmentV11)
                                                .getSegments();

    Assert.assertEquals(3, appendSegments.size());
    // Segment V11 is committed
    Assert.assertTrue(appendSegments.remove(segmentV11));
    // Segment V00 is also committed
    Assert.assertTrue(appendSegments.remove(segmentV00));
    // Segment V00 is upgraded to v1 with MONTH granularlity at the time of commit as V12
    final DataSegment segmentV12 = Iterables.getOnlyElement(appendSegments);
    Assert.assertEquals(v1, segmentV12.getVersion());
    Assert.assertEquals(JAN_23, segmentV12.getInterval());
    Assert.assertEquals(segmentV00.getLoadSpec(), segmentV12.getLoadSpec());

    verifyIntervalHasUsedSegments(JAN_23, segmentV00, segmentV10, segmentV11, segmentV12);
    verifyIntervalHasVisibleSegments(JAN_23, segmentV10, segmentV11, segmentV12);
  }


  @Nullable
  private DataSegment findSegmentWith(String version, Map<String, Object> loadSpec, Set<DataSegment> segments)
  {
    for (DataSegment segment : segments) {
      if (version.equals(segment.getVersion())
          && Objects.equals(segment.getLoadSpec(), loadSpec)) {
        return segment;
      }
    }

    return null;
  }

  private static DataSegment asSegment(SegmentIdWithShardSpec pendingSegment)
  {
    final SegmentId id = pendingSegment.asSegmentId();
    return new DataSegment(
        id,
        Collections.singletonMap(id.toString(), id.toString()),
        Collections.emptyList(),
        Collections.emptyList(),
        pendingSegment.getShardSpec(),
        null,
        0,
        0
    );
  }

  private void verifyIntervalHasUsedSegments(Interval interval, DataSegment... expectedSegments)
  {
    verifySegments(interval, Segments.INCLUDING_OVERSHADOWED, expectedSegments);
  }

  private void verifyIntervalHasVisibleSegments(Interval interval, DataSegment... expectedSegments)
  {
    verifySegments(interval, Segments.ONLY_VISIBLE, expectedSegments);
  }

  private void verifySegments(Interval interval, Segments visibility, DataSegment... expectedSegments)
  {
    try {
      Collection<DataSegment> allUsedSegments = dummyTaskActionClient.submit(
          new RetrieveUsedSegmentsAction(
              WIKI,
              null,
              ImmutableList.of(interval),
              visibility
          )
      );
      Assert.assertEquals(Sets.newHashSet(expectedSegments), Sets.newHashSet(allUsedSegments));
    }
    catch (IOException e) {
      throw new ISE(e, "Error while fetching used segments in interval[%s]", interval);
    }
  }

  private void verifyInputSegments(Task task, Interval interval, DataSegment... expectedSegments)
  {
    try {
      final TaskActionClient taskActionClient = taskActionClientFactory.create(task);
      Collection<DataSegment> allUsedSegments = taskActionClient.submit(
          new RetrieveUsedSegmentsAction(
              WIKI,
              Collections.singletonList(interval)
          )
      );
      Assert.assertEquals(Sets.newHashSet(expectedSegments), Sets.newHashSet(allUsedSegments));
    }
    catch (IOException e) {
      throw new ISE(e, "Error while fetching segments to replace in interval[%s]", interval);
    }
  }

  private TaskToolboxFactory createToolboxFactory(
      TaskConfig taskConfig,
      TaskActionClientFactory taskActionClientFactory
  )
  {
    TestTaskToolboxFactory.Builder builder = new TestTaskToolboxFactory.Builder()
        .setConfig(taskConfig)
        .setIndexIO(new IndexIO(getObjectMapper(), ColumnConfig.DEFAULT))
        .setTaskActionClientFactory(taskActionClientFactory);
    return new TestTaskToolboxFactory(builder)
    {
      @Override
      public TaskToolbox build(TaskConfig config, Task task)
      {
        return createTaskToolbox(config, task);
      }
    };
  }

  private DataSegment createSegment(Interval interval, String version)
  {
    return DataSegment.builder()
                      .dataSource(WIKI)
                      .interval(interval)
                      .version(version)
                      .size(100)
                      .build();
  }

  private ActionsTestTask createAndStartTask()
  {
    ActionsTestTask task = new ActionsTestTask(WIKI, "test_" + groupId.incrementAndGet(), taskActionClientFactory);
    taskQueue.add(task);
    runningTasks.add(task);
    return task;
  }

}
