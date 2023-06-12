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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.MultipleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskStorageDirTracker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.ThreadingTaskRunner;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConcurrentReplaceAndAppendTest extends IngestionTestBase
{
  private TaskQueue taskQueue;
  private TaskActionClient taskActionClient;

  private static final WorkerConfig WORKER_CONFIG = new WorkerConfig().setCapacity(10);

  @Before
  public void setup() throws Exception
  {
    final TaskConfig taskConfig = new TaskConfigBuilder().build();
    final TaskActionClientFactory taskActionClientFactory = createActionClientFactory();
    taskActionClient = taskActionClientFactory.create(NoopTask.create());
    final TaskToolboxFactory toolboxFactory = new TestTaskToolboxFactory(taskConfig, taskActionClientFactory);
    final TaskRunner taskRunner = new ThreadingTaskRunner(
        toolboxFactory,
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
        new TaskQueueConfig(null, new Period(0L), null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        taskRunner,
        taskActionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.start();
  }

  @Test
  public void test() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023/2024"),
        Granularities.YEAR,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023/2024"),
        Granularities.YEAR,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    ReplaceTask replaceTask1 = new ReplaceTask(
        "replace1",
        "DS",
        Intervals.of("2023/2024"),
        Granularities.YEAR,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    ReplaceTask replaceTask2 = new ReplaceTask(
        "replace2",
        "DS",
        Intervals.of("2023/2024"),
        Granularities.YEAR,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    // Create a set of initial segments
    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskSuccess(replaceTask0);

    // Append task begins and allocates pending segments
    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    appendTask0.allocateOrGetSegmentForTimestamp("2023-01-01");
    appendTask0.completeSegmentAllocation();

    // New replace task starts and ends before the appending task finishes
    taskQueue.add(replaceTask1);
    replaceTask1.markReady();
    replaceTask1.beginPublish();
    replaceTask1.awaitRunComplete();
    verifySegmentCount(2, 1);
    verifyTaskSuccess(replaceTask1);

    taskQueue.add(replaceTask2);
    replaceTask2.markReady();
    replaceTask2.awaitReadyComplete();

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(4, 2);
    verifyTaskSuccess(appendTask0);

    replaceTask2.beginPublish();
    replaceTask2.awaitRunComplete();
    verifySegmentCount(6, 2);
    verifyTaskSuccess(replaceTask2);
  }

  @Test
  public void testRRAA_dailyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(replaceTask0);


    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(62, 62);
    verifyTaskSuccess(appendTask0);
  }

  @Test
  public void testRAAR_dailyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(appendTask0);

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(93, 62);
    verifyTaskSuccess(replaceTask0);
  }

  @Test
  public void testRARA_dailyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();


    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(replaceTask0);

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(93, 62);
    verifyTaskSuccess(appendTask0);
  }

  @Test
  public void testARRA_dailyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(replaceTask0);

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(93, 62);
    verifyTaskSuccess(appendTask0);
  }

  @Test
  public void testARAR_dailyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(appendTask0);

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(93, 62);
    verifyTaskSuccess(replaceTask0);
  }

  @Test
  public void testAARR_dailyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(appendTask0);

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(62, 31);
    verifyTaskSuccess(replaceTask0);
  }



  @Test
  public void testRRAA_monthlyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskSuccess(replaceTask0);


    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(2, 2);
    verifyTaskSuccess(appendTask0);
  }

  @Test
  public void testRAAR_monthlyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(appendTask0);

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(63, 32);
    verifyTaskSuccess(replaceTask0);
  }

  @Test
  public void testRARA_monthlyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();


    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskSuccess(replaceTask0);

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(63, 32);
    verifyTaskSuccess(appendTask0);
  }

  @Test
  public void testARRA_monthlyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskSuccess(replaceTask0);

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(63, 32);
    verifyTaskSuccess(appendTask0);
  }

  @Test
  public void testARAR_monthlyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(appendTask0);

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(63, 32);
    verifyTaskSuccess(replaceTask0);
  }

  @Test
  public void testAARR_monthlyReplaceDailyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(appendTask0);

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(32, 1);
    verifyTaskSuccess(replaceTask0);
  }



  @Test
  public void testRRAA_dailyReplaceMonthlyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(replaceTask0);


    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(62, 62);
    verifyTaskSuccess(appendTask0);
  }

  @Test
  public void testRAAR_dailyReplaceMonthlyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskSuccess(appendTask0);

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskFailure(replaceTask0);
  }

  @Test
  public void testRARA_dailyReplaceMonthlyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();


    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(replaceTask0);

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskFailure(appendTask0);
  }

  @Test
  public void testARRA_dailyReplaceMonthlyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskSuccess(replaceTask0);

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(31, 31);
    verifyTaskFailure(appendTask0);
  }

  @Test
  public void testARAR_dailyReplaceMonthlyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskSuccess(appendTask0);

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskFailure(replaceTask0);
  }

  @Test
  public void testAARR_dailyReplaceMonthlyAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    for (int i = 1; i <= 9; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-0" + i);
    }
    for (int i = 10; i <= 31; i++) {
      appendTask0.allocateOrGetSegmentForTimestamp("2023-01-" + i);
    }
    appendTask0.completeSegmentAllocation();
    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskSuccess(appendTask0);

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(32, 31);
    verifyTaskSuccess(replaceTask0);
  }

  @Test
  public void testMultipleAppend() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023/2024"),
        Granularities.YEAR,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023/2024"),
        Granularities.YEAR,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    AppendTask appendTask1 = new AppendTask(
        "append1",
        "DS",
        Intervals.of("2023/2024"),
        Granularities.YEAR,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    appendTask0.allocateOrGetSegmentForTimestamp("2023-01-01");
    appendTask0.completeSegmentAllocation();


    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();

    taskQueue.add(appendTask1);
    appendTask1.markReady();
    appendTask1.awaitReadyComplete();
    appendTask1.allocateOrGetSegmentForTimestamp("2023-01-01");
    appendTask1.completeSegmentAllocation();
    appendTask1.beginPublish();
    appendTask1.awaitRunComplete();
    verifySegmentCount(1, 1);
    verifyTaskSuccess(appendTask1);

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(3, 2);
    verifyTaskSuccess(replaceTask0);

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(5, 3);
    verifyTaskSuccess(appendTask0);
  }

  @Test
  public void testMultipleGranularities() throws Exception
  {
    ReplaceTask replaceTask0 = new ReplaceTask(
        "replace0",
        "DS",
        Intervals.of("2023/2024"),
        Granularities.YEAR,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "REPLACE",
            "numCorePartitions", 1
        )
    );

    AppendTask appendTask0 = new AppendTask(
        "append0",
        "DS",
        Intervals.of("2023-01-01/2023-02-01"),
        Granularities.DAY,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    AppendTask appendTask1 = new AppendTask(
        "append1",
        "DS",
        Intervals.of("2023-07-01/2024-01-01"),
        Granularities.QUARTER,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    AppendTask appendTask2 = new AppendTask(
        "append2",
        "DS",
        Intervals.of("2023-12-01/2024-01-01"),
        Granularities.MONTH,
        ImmutableMap.of(
            Tasks.TASK_LOCK_TYPE, "APPEND",
            "numPartitions", 1
        )
    );

    taskQueue.add(appendTask0);
    appendTask0.markReady();
    appendTask0.awaitReadyComplete();
    appendTask0.allocateOrGetSegmentForTimestamp("2023-01-01");
    appendTask0.completeSegmentAllocation();


    taskQueue.add(appendTask1);
    appendTask1.markReady();
    appendTask1.awaitReadyComplete();
    appendTask1.allocateOrGetSegmentForTimestamp("2023-07-01");
    appendTask1.allocateOrGetSegmentForTimestamp("2023-08-01");
    appendTask1.allocateOrGetSegmentForTimestamp("2023-09-01");
    appendTask1.allocateOrGetSegmentForTimestamp("2023-10-01");
    appendTask1.allocateOrGetSegmentForTimestamp("2023-11-01");
    appendTask1.allocateOrGetSegmentForTimestamp("2023-12-01");
    appendTask1.completeSegmentAllocation();
    appendTask1.beginPublish();
    appendTask1.awaitRunComplete();
    verifySegmentCount(2, 2);
    verifyTaskSuccess(appendTask1);

    taskQueue.add(appendTask2);
    appendTask2.markReady();
    appendTask2.awaitReadyComplete();
    appendTask2.allocateOrGetSegmentForTimestamp("2023-12-01");
    appendTask2.completeSegmentAllocation();

    taskQueue.add(replaceTask0);
    replaceTask0.markReady();
    replaceTask0.awaitReadyComplete();

    appendTask0.beginPublish();
    appendTask0.awaitRunComplete();
    verifySegmentCount(3, 3);
    verifyTaskSuccess(appendTask0);

    replaceTask0.beginPublish();
    replaceTask0.awaitRunComplete();
    verifySegmentCount(5, 2);
    verifyTaskSuccess(replaceTask0);

    appendTask2.beginPublish();
    appendTask2.awaitRunComplete();
    verifySegmentCount(7, 3);
    verifyTaskSuccess(appendTask2);
  }

  private void verifyTaskSuccess(Task task)
  {
    try {
      while (!getTaskStorage().getStatus(task.getId()).get().isComplete()) {
        Thread.sleep(100);
      }
    }
    catch (InterruptedException e) {
      // do nothing
    }
    Assert.assertTrue(getTaskStorage().getStatus(task.getId()).get().isSuccess());
  }

  private void verifyTaskFailure(Task task)
  {
    try {
      while (!getTaskStorage().getStatus(task.getId()).get().isComplete()) {
        Thread.sleep(100);
      }
    }
    catch (InterruptedException e) {
      // do nothing
    }
    Assert.assertTrue(getTaskStorage().getStatus(task.getId()).get().isFailure());
  }

  private void verifySegmentCount(int expectedTotal, int expectedVisible) throws Exception
  {
    Collection<DataSegment> allUsed = taskActionClient.submit(
        new RetrieveUsedSegmentsAction("DS", null, ImmutableList.of(Intervals.ETERNITY), Segments.INCLUDING_OVERSHADOWED)
    );
    System.out.println("All used segments: " + allUsed.size());
    System.out.println(new TreeSet<>(allUsed.stream().map(s -> s.getId().toString()).collect(Collectors.toSet())));
    Collection<DataSegment> visibleUsed = taskActionClient.submit(
        new RetrieveUsedSegmentsAction("DS", null, ImmutableList.of(Intervals.ETERNITY), Segments.ONLY_VISIBLE)
    );
    Assert.assertEquals(expectedTotal, allUsed.size());
    System.out.println("All visible segments: " + visibleUsed.size());
    System.out.println(new TreeSet<>(visibleUsed.stream().map(s -> s.getId().toString()).collect(Collectors.toSet())));
    Assert.assertEquals(expectedVisible, visibleUsed.size());
  }

  private class TestTaskToolboxFactory extends TaskToolboxFactory
  {
    private final TaskConfig taskConfig;

    public TestTaskToolboxFactory(TaskConfig taskConfig, TaskActionClientFactory taskActionClientFactory)
    {
      super(
          taskConfig,
          null,
          taskActionClientFactory,
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
          null,
          null,
          null,
          null,
          new IndexIO(getObjectMapper(), () -> 0),
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
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );
      this.taskConfig = taskConfig;
    }

    @Override
    public TaskToolbox build(Task task)
    {
      return build(taskConfig, task);
    }

    @Override
    public TaskToolbox build(Function<TaskConfig, TaskConfig> decoratorFn, Task task)
    {
      return build(decoratorFn.apply(taskConfig), task);
    }


    @Override
    public TaskToolbox build(TaskConfig config, Task task)
    {
      try {
        return createTaskToolbox(config, task);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
