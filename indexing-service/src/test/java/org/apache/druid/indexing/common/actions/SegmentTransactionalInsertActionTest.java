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

package org.apache.druid.indexing.common.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.assertj.core.api.Assertions;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.util.Map;

@RunWith(EasyMockRunner.class)
public class SegmentTransactionalInsertActionTest extends EasyMockSupport
{
  @Rule
  public TaskActionTestKit actionTestKit = new TaskActionTestKit();

  private static final String DATA_SOURCE = "none";
  private static final Interval INTERVAL = Intervals.of("2020/2020T01");
  private static final String PARTY_YEAR = "1999";
  private static final String THE_DISTANT_FUTURE = "3000";

  private static final DataSegment SEGMENT1 = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      PARTY_YEAR,
      ImmutableMap.of(),
      ImmutableList.of(),
      ImmutableList.of(),
      new LinearShardSpec(0),
      9,
      1024
  );

  private static final DataSegment SEGMENT2 = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      PARTY_YEAR,
      ImmutableMap.of(),
      ImmutableList.of(),
      ImmutableList.of(),
      new LinearShardSpec(1),
      9,
      1024
  );

  private static final DataSegment SEGMENT3 = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      THE_DISTANT_FUTURE,
      ImmutableMap.of(),
      ImmutableList.of(),
      ImmutableList.of(),
      new LinearShardSpec(1),
      9,
      1024
  );

  @Mock
  private TaskActionToolbox taskActionToolbox;

  @Mock
  SupervisorManager supervisorManager;

  private LockResult acquireTimeChunkLock(TaskLockType lockType, Task task, Interval interval, long timeoutMs)
      throws InterruptedException
  {
    return actionTestKit.getTaskLockbox().lock(task, new TimeChunkLockRequest(lockType, task, interval, null), timeoutMs);
  }

  @Test
  public void testTransactionalUpdateDataSourceMetadata() throws Exception
  {
    final Task task = NoopTask.create();
    actionTestKit.getTaskLockbox().add(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    SegmentPublishResult result1 = SegmentTransactionalInsertAction.appendAction(
        ImmutableSet.of(SEGMENT1),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableList.of(1)),
        null
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT1)), result1);

    SegmentPublishResult result2 = SegmentTransactionalInsertAction.appendAction(
        ImmutableSet.of(SEGMENT2),
        new ObjectMetadata(ImmutableList.of(1)),
        new ObjectMetadata(ImmutableList.of(2)),
        null
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT2)), result2);

    Assertions.assertThat(
        actionTestKit.getMetadataStorageCoordinator()
                     .retrieveUsedSegmentsForInterval(DATA_SOURCE, INTERVAL, Segments.ONLY_VISIBLE)
    ).containsExactlyInAnyOrder(SEGMENT1, SEGMENT2);

    Assert.assertEquals(
        new ObjectMetadata(ImmutableList.of(2)),
        actionTestKit.getMetadataStorageCoordinator().retrieveDataSourceMetadata(DATA_SOURCE)
    );
  }

  @Test
  public void testFailTransactionalUpdateDataSourceMetadata() throws Exception
  {
    final Task task = NoopTask.create();
    actionTestKit.getTaskLockbox().add(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    SegmentPublishResult result = SegmentTransactionalInsertAction.appendAction(
        ImmutableSet.of(SEGMENT1),
        new ObjectMetadata(ImmutableList.of(1)),
        new ObjectMetadata(ImmutableList.of(2)),
        null
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );

    Assert.assertEquals(
        SegmentPublishResult.fail(
            InvalidInput.exception(
                "The new start metadata state[ObjectMetadata{theObject=[1]}] is"
                + " ahead of the last committed end state[null]. Try resetting the supervisor."
            ).toString()
        ),
        result
    );
  }

  @Test
  public void testFailBadVersion() throws Exception
  {
    final Task task = NoopTask.create();
    final SegmentTransactionalInsertAction action = SegmentTransactionalInsertAction
        .overwriteAction(null, ImmutableSet.of(SEGMENT3), null);
    actionTestKit.getTaskLockbox().add(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    IllegalStateException exception = Assert.assertThrows(
        IllegalStateException.class,
        () -> action.perform(task, actionTestKit.getTaskActionToolbox())
    );
    Assert.assertTrue(exception.getMessage().contains("are not covered by locks"));
  }

  @Test
  public void testStreamingTaskNotPublishable() throws Exception
  {
    // Mocking the config classes because they have a lot of logic in their constructors that we don't really want here.
    SeekableStreamIndexTaskTuningConfig taskTuningConfig = EasyMock.createMock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig taskIOConfig = EasyMock.createMock(SeekableStreamIndexTaskIOConfig.class);

    final SeekableStreamIndexTask streamingTask = new TestSeekableStreamIndexTask(
        "id1",
        null,
        DataSchema.builder().withDataSource(DATA_SOURCE).build(),
        taskTuningConfig,
        taskIOConfig,
        ImmutableMap.of(),
        "0"
    );

    EasyMock.expect(taskActionToolbox.getSupervisorManager()).andReturn(supervisorManager);
    EasyMock.expect(taskActionToolbox.getTaskLockbox()).andReturn(actionTestKit.getTaskLockbox());
    EasyMock.expect(supervisorManager.canPublishSegments(EasyMock.anyString(), EasyMock.anyInt(), EasyMock.anyString()))
            .andReturn(false);

    actionTestKit.getTaskLockbox().add(streamingTask);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, streamingTask, INTERVAL, 5000);
    replayAll();

    DruidException druidException = Assert.assertThrows(DruidException.class, () -> SegmentTransactionalInsertAction.appendAction(
            ImmutableSet.of(SEGMENT1),
            new ObjectMetadata(null),
            new ObjectMetadata(ImmutableList.of(1)),
            null
        ).perform(
            streamingTask,
            taskActionToolbox
    ));
    verifyAll();

    Assert.assertEquals(503, druidException.getStatusCode());
  }

  private static class TestSeekableStreamIndexTask extends SeekableStreamIndexTask<String, String, ByteEntity>
  {
    public TestSeekableStreamIndexTask(
        String id,
        @Nullable TaskResource taskResource,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig,
        SeekableStreamIndexTaskIOConfig<String, String> ioConfig,
        @Nullable Map<String, Object> context,
        @Nullable String groupId
    )
    {
      super(
          id,
          taskResource,
          dataSchema,
          tuningConfig,
          ioConfig,
          context,
          groupId
      );
    }

    @Override
    protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
    {
      return null;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> newTaskRecordSupplier(final TaskToolbox toolbox)
    {
      return null;
    }

    @Override
    public String getType()
    {
      return "test";
    }
  }

}
