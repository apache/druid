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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.common.MultipleFileTaskReportFileWriter;
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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Contains tests to verify behaviour of concurrently running REPLACE and APPEND
 * tasks on the same interval of a datasource.
 * <p>
 * The tests verify the interleaving of the following actions:
 * <ul>
 *   <li>LOCK: Acquisiting of a lock on an interval by a replace task</li>
 *   <li>ALLOCATE: Allocation of a pending segment by an append task</li>
 *   <li>REPLACE: Commit of segments created by a replace task</li>
 *   <li>APPEND: Commit of segments created by an append task</li>
 * </ul>
 */
public class ConcurrentReplaceAppendTest extends IngestionTestBase
{
  /**
   * The version used by append jobs when no previous replace job has run on an interval.
   */
  private static final String SEGMENT_V0 = "1970-01-01T00:00:00.000Z";

  private static final Interval YEAR_23 = Intervals.of("2023/2024");
  private static final Interval JAN_23 = Intervals.of("2023-01/2023-02");
  private static final Interval FIRST_OF_JAN_23 = Intervals.of("2023-01-01/2023-01-02");

  private static final String WIKI = "wiki";

  private TaskQueue taskQueue;
  private TaskActionClientFactory taskActionClientFactory;
  private TaskActionClient dummyTaskActionClient;
  private final List<ActionsTestTask> runningTasks = new ArrayList<>();

  private ActionsTestTask appendTask;
  private ActionsTestTask replaceTask;

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
    replaceTask.finishRunAndGetStatus();
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);

    appendTask.acquireAppendLockOn(FIRST_OF_JAN_23);
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(segmentV10.getVersion(), pendingSegment.getVersion());

    final DataSegment segmentV11 = asSegment(pendingSegment);
    appendTask.commitAppendSegments(segmentV11);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10, segmentV11);
  }

  @Test
  public void testLockAllocateAppendReplace()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    appendTask.acquireAppendLockOn(FIRST_OF_JAN_23).getVersion();
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

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
  public void testLockAllocateReplaceAppend()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    appendTask.acquireAppendLockOn(FIRST_OF_JAN_23).getVersion();
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    replaceTask.finishRunAndGetStatus();

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
  public void testAllocateLockReplaceAppend()
  {
    appendTask.acquireAppendLockOn(FIRST_OF_JAN_23).getVersion();
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(SEGMENT_V0, pendingSegment.getVersion());

    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    replaceTask.commitReplaceSegments(segmentV10);
    replaceTask.finishRunAndGetStatus();

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
  public void testAllocateLockAppendReplace()
  {
    appendTask.acquireAppendLockOn(FIRST_OF_JAN_23).getVersion();
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
  public void testAllocateAppendLockReplace()
  {
    appendTask.acquireAppendLockOn(FIRST_OF_JAN_23).getVersion();
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
    replaceTask.finishRunAndGetStatus();

    // Verify that the segment appended to v0 gets fully overshadowed
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);
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
    appendTask1.acquireAppendLockOn(YEAR_23);
    final SegmentIdWithShardSpec pendingSegmentV11
        = appendTask1.allocateSegmentForTimestamp(YEAR_23.getStart(), Granularities.YEAR);
    Assert.assertEquals(segmentV10.getVersion(), pendingSegmentV11.getVersion());

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
    ActionsTestTask task = new ActionsTestTask(WIKI, taskActionClientFactory);
    taskQueue.add(task);
    runningTasks.add(task);
    return task;
  }

}
