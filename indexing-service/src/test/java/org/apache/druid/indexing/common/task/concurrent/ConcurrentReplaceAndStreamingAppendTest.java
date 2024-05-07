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

import com.google.common.base.Optional;
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
import org.apache.druid.indexing.common.task.NoopTaskContextEnricher;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TestTaskToolboxFactory;
import org.apache.druid.indexing.overlord.ThreadingTaskRunner;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
public class ConcurrentReplaceAndStreamingAppendTest extends IngestionTestBase
{
  /**
   * The version used by append jobs when no previous replace job has run on an interval.
   */
  private static final String SEGMENT_V0 = DateTimes.EPOCH.toString();

  private static final Interval JAN_23 = Intervals.of("2023-01/2023-02");
  private static final Interval FIRST_OF_JAN_23 = Intervals.of("2023-01-01/2023-01-02");

  private static final String WIKI = "wiki";

  private TaskQueue taskQueue;
  private TaskActionClientFactory taskActionClientFactory;
  private TaskActionClient dummyTaskActionClient;
  private final List<ActionsTestTask> runningTasks = new ArrayList<>();

  private ActionsTestTask appendTask;
  private ActionsTestTask replaceTask;

  private final AtomicInteger groupId = new AtomicInteger(0);
  private final SupervisorManager supervisorManager = EasyMock.mock(SupervisorManager.class);
  private Capture<String> supervisorId;
  private Capture<PendingSegmentRecord> pendingSegment;
  private Map<String, Map<Interval, Set<Object>>> versionToIntervalToLoadSpecs;
  private Map<String, Object> parentSegmentToLoadSpec;

  @Override
  @Before
  public void setUpIngestionTestBase() throws IOException
  {
    EasyMock.reset(supervisorManager);
    EasyMock.expect(supervisorManager.getActiveSupervisorIdForDatasourceWithAppendLock(WIKI))
            .andReturn(Optional.of(WIKI)).anyTimes();
    super.setUpIngestionTestBase();
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
        new NoopServiceEmitter(),
        getObjectMapper(),
        new NoopTaskContextEnricher()
    );
    runningTasks.clear();
    taskQueue.start();

    groupId.set(0);
    appendTask = createAndStartTask();
    supervisorId = Capture.newInstance(CaptureType.ALL);
    pendingSegment = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(supervisorManager.registerUpgradedPendingSegmentOnSupervisor(
        EasyMock.capture(supervisorId),
        EasyMock.capture(pendingSegment)
    )).andReturn(true).anyTimes();
    replaceTask = createAndStartTask();
    EasyMock.replay(supervisorManager);
    versionToIntervalToLoadSpecs = new HashMap<>();
    parentSegmentToLoadSpec = new HashMap<>();
  }

  @After
  public void tearDown()
  {
    verifyVersionIntervalLoadSpecUniqueness();
    for (ActionsTestTask task : runningTasks) {
      task.finishRunAndGetStatus();
    }
  }

  @Test
  public void testLockReplaceAllocateAppend()
  {
    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);

    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(segmentV10.getVersion(), pendingSegment.getVersion());

    final DataSegment segmentV11 = asSegment(pendingSegment);
    commitAppendSegments(segmentV11);

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
    commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    commitReplaceSegments(segmentV10);

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
    commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    commitAppendSegments(segmentV01);

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
    commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    commitAppendSegments(segmentV01);

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
    commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    commitReplaceSegments(segmentV10);
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
    commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final String v1 = replaceTask.acquireReplaceLockOn(FIRST_OF_JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    commitReplaceSegments(segmentV10);

    // Verify that the segment appended to v0 gets fully overshadowed
    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);
  }

  @Test
  public void testLockReplaceMonthAllocateAppendDay()
  {
    String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    // Verify that the allocated segment takes the version and interval of previous replace
    final SegmentIdWithShardSpec pendingSegment
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(JAN_23, pendingSegment.getInterval());
    Assert.assertEquals(v1, pendingSegment.getVersion());

    final DataSegment segmentV11 = asSegment(pendingSegment);
    commitAppendSegments(segmentV11);

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
    commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    commitReplaceSegments(segmentV10);

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
    commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    commitAppendSegments(segmentV01);

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
    commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    commitAppendSegments(segmentV01);

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
    commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    commitReplaceSegments(segmentV10);

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
    commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    commitReplaceSegments(segmentV10);

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
    commitAppendSegments(segmentV11);

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
    commitAppendSegments(segmentV01);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV01);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV01);

    final DataSegment segmentV10 = createSegment(FIRST_OF_JAN_23, v1);
    commitReplaceSegments(segmentV10);

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
    commitReplaceSegments(segmentV10);

    verifyIntervalHasUsedSegments(FIRST_OF_JAN_23, segmentV10);
    verifyIntervalHasVisibleSegments(FIRST_OF_JAN_23, segmentV10);

    final DataSegment segmentV01 = asSegment(pendingSegment);
    commitAppendSegments(segmentV01);

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
    final ISE exception = Assert.assertThrows(ISE.class, () -> commitReplaceSegments(segmentV10));
    final Throwable throwable = Throwables.getRootCause(exception);
    Assert.assertEquals(
        StringUtils.format(
            "Segments[[%s]] are not covered by locks[[]] for task[%s]",
            segmentV10, replaceTask.getId()
        ),
        throwable.getMessage()
    );

    final DataSegment segmentV01 = asSegment(pendingSegment);
    commitAppendSegments(segmentV01);
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
  public void testLockAllocateDayReplaceMonthAllocateAppend()
  {
    final SegmentIdWithShardSpec pendingSegmentV0
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);

    final String v1 = replaceTask.acquireReplaceLockOn(JAN_23).getVersion();

    final DataSegment segmentV10 = createSegment(JAN_23, v1);
    commitReplaceSegments(segmentV10);
    verifyIntervalHasUsedSegments(JAN_23, segmentV10);

    final SegmentIdWithShardSpec pendingSegmentV1
        = appendTask.allocateSegmentForTimestamp(FIRST_OF_JAN_23.getStart(), Granularities.DAY);
    Assert.assertEquals(segmentV10.getVersion(), pendingSegmentV1.getVersion());

    final DataSegment segmentV00 = asSegment(pendingSegmentV0);
    final DataSegment segmentV11 = asSegment(pendingSegmentV1);
    Set<DataSegment> appendSegments = commitAppendSegments(segmentV00, segmentV11)
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
        return createTaskToolbox(config, task, supervisorManager);
      }
    };
  }

  private DataSegment createSegment(Interval interval, String version)
  {
    SegmentId id = SegmentId.of(WIKI, interval, version, null);
    return DataSegment.builder()
                      .dataSource(WIKI)
                      .interval(interval)
                      .version(version)
                      .loadSpec(Collections.singletonMap(id.toString(), id.toString()))
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

  private void commitReplaceSegments(DataSegment... dataSegments)
  {
    replaceTask.commitReplaceSegments(dataSegments);
    for (int i = 0; i < supervisorId.getValues().size(); i++) {
      announceUpgradedPendingSegment(pendingSegment.getValues().get(i));
    }
    supervisorId.reset();
    pendingSegment.reset();
    replaceTask.finishRunAndGetStatus();
  }

  private SegmentPublishResult commitAppendSegments(DataSegment... dataSegments)
  {
    SegmentPublishResult result = appendTask.commitAppendSegments(dataSegments);
    result.getSegments().forEach(this::unannounceUpgradedPendingSegment);
    for (DataSegment segment : dataSegments) {
      parentSegmentToLoadSpec.put(segment.getId().toString(), Iterables.getOnlyElement(segment.getLoadSpec().values()));
    }
    appendTask.finishRunAndGetStatus();
    return result;
  }

  private void announceUpgradedPendingSegment(PendingSegmentRecord pendingSegment)
  {
    appendTask.getAnnouncedSegmentsToParentSegments()
              .put(pendingSegment.getId().asSegmentId(), pendingSegment.getUpgradedFromSegmentId());
  }

  private void unannounceUpgradedPendingSegment(
      DataSegment segment
  )
  {
    appendTask.getAnnouncedSegmentsToParentSegments()
              .remove(segment.getId());
  }

  private void verifyVersionIntervalLoadSpecUniqueness()
  {
    for (DataSegment usedSegment : getAllUsedSegments()) {
      final String version = usedSegment.getVersion();
      final Interval interval = usedSegment.getInterval();
      final Object loadSpec = Iterables.getOnlyElement(usedSegment.getLoadSpec().values());
      Map<Interval, Set<Object>> intervalToLoadSpecs
          = versionToIntervalToLoadSpecs.computeIfAbsent(version, v -> new HashMap<>());
      Set<Object> loadSpecs
          = intervalToLoadSpecs.computeIfAbsent(interval, i -> new HashSet<>());
      Assert.assertFalse(loadSpecs.contains(loadSpec));
      loadSpecs.add(loadSpec);
    }

    for (Map.Entry<SegmentId, String> entry : appendTask.getAnnouncedSegmentsToParentSegments().entrySet()) {
      final String version = entry.getKey().getVersion();
      final Interval interval = entry.getKey().getInterval();
      final Object loadSpec = parentSegmentToLoadSpec.get(entry.getValue());
      Map<Interval, Set<Object>> intervalToLoadSpecs
          = versionToIntervalToLoadSpecs.computeIfAbsent(version, v -> new HashMap<>());
      Set<Object> loadSpecs
          = intervalToLoadSpecs.computeIfAbsent(interval, i -> new HashSet<>());
      Assert.assertFalse(loadSpecs.contains(loadSpec));
      loadSpecs.add(loadSpec);
    }
  }

  private Collection<DataSegment> getAllUsedSegments()
  {
    try {
      return dummyTaskActionClient.submit(
          new RetrieveUsedSegmentsAction(
              WIKI,
              null,
              ImmutableList.of(Intervals.ETERNITY),
              Segments.INCLUDING_OVERSHADOWED
          )
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
