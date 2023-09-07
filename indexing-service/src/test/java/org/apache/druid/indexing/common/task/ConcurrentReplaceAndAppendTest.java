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

package org.apache.druid.indexing.common.task;

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
import org.apache.druid.indexing.common.task.batch.parallel.ActionsTestTask;
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

public class ConcurrentReplaceAndAppendTest extends IngestionTestBase
{
  private static final WorkerConfig WORKER_CONFIG = new WorkerConfig().setCapacity(10);

  private TaskQueue taskQueue;
  private TaskRunner taskRunner;
  private TaskActionClientFactory taskActionClientFactory;
  private TaskActionClient dummyTaskActionClient;
  private final List<Task> runningTasks = new ArrayList<>();

  @Before
  public void setup()
  {
    final TaskConfig taskConfig = new TaskConfigBuilder().build();
    taskActionClientFactory = createActionClientFactory();
    dummyTaskActionClient = taskActionClientFactory.create(NoopTask.create());
    taskRunner = new ThreadingTaskRunner(
        createToolboxFactory(taskConfig, taskActionClientFactory),
        taskConfig,
        WORKER_CONFIG,
        new NoopTaskLogs(),
        getObjectMapper(),
        new TestAppenderatorsManager(),
        new MultipleFileTaskReportFileWriter(),
        new DruidNode("middleManager", "host", false, 8091, null, true, false),
        TaskStorageDirTracker.fromConfigs(WORKER_CONFIG, taskConfig)
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
  }

  @After
  public void tearDown()
  {
    for (Task task : runningTasks) {
      if (task instanceof ActionsTestTask) {
        ((ActionsTestTask) task).finishRunAndGetStatus();
      }
    }
  }

  @Test
  public void testAppendSegmentGetsUpgraded()
  {
    final Interval year2023 = Intervals.of("2023/2024");

    // Commit initial segments for v0
    final ActionsTestTask replaceTask0 = createAndStartTask();
    final String v0 = replaceTask0.acquireReplaceLockOn(year2023).getVersion();

    final DataSegment segmentV00 = createSegment(year2023, v0);
    replaceTask0.commitReplaceSegments(segmentV00);
    replaceTask0.finishRunAndGetStatus();
    verifyIntervalHasUsedSegments(year2023, segmentV00);
    verifyIntervalHasVisibleSegments(year2023, segmentV00);

    // Allocate an append segment for v0
    final ActionsTestTask appendTask0 = createAndStartTask();
    appendTask0.acquireAppendLockOn(year2023);
    final SegmentIdWithShardSpec pendingSegmentV01
        = appendTask0.allocateSegmentForTimestamp(year2023.getStart(), Granularities.YEAR);
    Assert.assertEquals(segmentV00.getVersion(), pendingSegmentV01.getVersion());

    // Commit replace segment for v1
    final ActionsTestTask replaceTask1 = createAndStartTask();
    final String v1 = replaceTask1.acquireReplaceLockOn(year2023).getVersion();

    final DataSegment segmentV10 = DataSegment.builder(segmentV00).version(v1).build();
    replaceTask1.commitReplaceSegments(segmentV10);
    replaceTask1.finishRunAndGetStatus();
    verifyIntervalHasUsedSegments(year2023, segmentV00, segmentV10);
    verifyIntervalHasVisibleSegments(year2023, segmentV10);

    final ActionsTestTask replaceTask2 = createAndStartTask();
    final String v2 = replaceTask2.acquireReplaceLockOn(year2023).getVersion();

    // Commit append segment v0 and verify that it gets upgraded to v1
    final DataSegment segmentV01 = asSegment(pendingSegmentV01);
    final DataSegment segmentV11 = DataSegment.builder(segmentV01).version(v1).build();
    Set<DataSegment> appendedSegments = appendTask0.commitAppendSegments(segmentV01).getSegments();
    Assert.assertEquals(Sets.newHashSet(segmentV01, segmentV11), appendedSegments);

    appendTask0.finishRunAndGetStatus();
    verifyIntervalHasUsedSegments(
        year2023,
        segmentV00, segmentV01, segmentV10, segmentV11
    );
    verifyIntervalHasVisibleSegments(year2023, segmentV10, segmentV11);

    // Commit replace segment v2 and verify that append segment gets upgraded to v2
    final DataSegment segmentV20 = DataSegment.builder(segmentV00).version(v2).build();
    replaceTask2.commitReplaceSegments(segmentV20);
    replaceTask2.finishRunAndGetStatus();

    final DataSegment segmentV21 = DataSegment.builder(segmentV01).version(v2).build();
    verifyIntervalHasUsedSegments(
        year2023,
        segmentV00, segmentV01, segmentV10, segmentV11, segmentV20, segmentV21
    );
    verifyIntervalHasVisibleSegments(year2023, segmentV20, segmentV21);
  }

  @Test
  public void testRRAA_dailyReplaceDailyAppend()
  {
    final Interval firstOfJan = Intervals.of("2023-01-01/2023-01-02");

    final ActionsTestTask replaceTask0 = createAndStartTask();
    final ActionsTestTask appendTask0 = createAndStartTask();

    final String v0 = replaceTask0.acquireReplaceLockOn(firstOfJan).getVersion();

    final DataSegment segmentV00 = createSegment(firstOfJan, v0);

    replaceTask0.commitReplaceSegments(segmentV00);
    replaceTask0.finishRunAndGetStatus();
    verifyIntervalHasUsedSegments(firstOfJan, segmentV00);

    appendTask0.acquireAppendLockOn(firstOfJan);
    final SegmentIdWithShardSpec pendingSegment
        = appendTask0.allocateSegmentForTimestamp(firstOfJan.getStart(), Granularities.DAY);
    Assert.assertEquals(segmentV00.getVersion(), pendingSegment.getVersion());

    final DataSegment segmentV01 = asSegment(pendingSegment);
    appendTask0.commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(firstOfJan, segmentV00, segmentV01);
    verifyIntervalHasVisibleSegments(firstOfJan, segmentV00, segmentV01);
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
              DS.WIKI,
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
                      .dataSource(DS.WIKI)
                      .interval(interval)
                      .version(version)
                      .size(100)
                      .build();
  }

  private ActionsTestTask createAndStartTask()
  {
    ActionsTestTask task = new ActionsTestTask(DS.WIKI, taskActionClientFactory);
    taskQueue.add(task);
    runningTasks.add(task);
    return task;
  }

  private static class DS
  {
    static final String WIKI = "wiki";
  }

}
