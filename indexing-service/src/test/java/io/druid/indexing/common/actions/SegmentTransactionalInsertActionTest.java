/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ObjectMetadata;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.java.util.common.Intervals;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SegmentTransactionalInsertActionTest
{
  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
      ImmutableMap.<String, Object>of(),
      ImmutableList.<String>of(),
      ImmutableList.<String>of(),
      new LinearShardSpec(0),
      9,
      1024
  );

  private static final DataSegment SEGMENT2 = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      PARTY_YEAR,
      ImmutableMap.<String, Object>of(),
      ImmutableList.<String>of(),
      ImmutableList.<String>of(),
      new LinearShardSpec(1),
      9,
      1024
  );

  private static final DataSegment SEGMENT3 = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      THE_DISTANT_FUTURE,
      ImmutableMap.<String, Object>of(),
      ImmutableList.<String>of(),
      ImmutableList.<String>of(),
      new LinearShardSpec(1),
      9,
      1024
  );

  @Test
  public void testTransactional() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);
    actionTestKit.getTaskLockbox().add(task);
    actionTestKit.getTaskLockbox().lock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    SegmentPublishResult result1 = new SegmentTransactionalInsertAction(
        ImmutableSet.of(SEGMENT1),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableList.of(1))
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(SEGMENT1), true), result1);

    SegmentPublishResult result2 = new SegmentTransactionalInsertAction(
        ImmutableSet.of(SEGMENT2),
        new ObjectMetadata(ImmutableList.of(1)),
        new ObjectMetadata(ImmutableList.of(2))
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(SEGMENT2), true), result2);

    Assert.assertEquals(
        ImmutableSet.of(SEGMENT1, SEGMENT2),
        ImmutableSet.copyOf(
            actionTestKit.getMetadataStorageCoordinator()
                         .getUsedSegmentsForInterval(DATA_SOURCE, INTERVAL)
        )
    );

    Assert.assertEquals(
        new ObjectMetadata(ImmutableList.of(2)),
        actionTestKit.getMetadataStorageCoordinator().getDataSourceMetadata(DATA_SOURCE)
    );
  }

  @Test
  public void testFailTransactional() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);
    actionTestKit.getTaskLockbox().add(task);
    actionTestKit.getTaskLockbox().lock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    SegmentPublishResult result = new SegmentTransactionalInsertAction(
        ImmutableSet.of(SEGMENT1),
        new ObjectMetadata(ImmutableList.of(1)),
        new ObjectMetadata(ImmutableList.of(2))
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );

    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.<DataSegment>of(), false), result);
  }

  @Test
  public void testFailBadVersion() throws Exception
  {
    final Task task = new NoopTask(null, null, 0, 0, null, null, null);
    final SegmentTransactionalInsertAction action = new SegmentTransactionalInsertAction(ImmutableSet.of(SEGMENT3));
    actionTestKit.getTaskLockbox().add(task);
    actionTestKit.getTaskLockbox().lock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(CoreMatchers.startsWith("Segments not covered by locks for task"));
    SegmentPublishResult result = action.perform(task, actionTestKit.getTaskActionToolbox());
    Assert.assertEquals(new SegmentPublishResult(ImmutableSet.of(SEGMENT3), true), result);
  }
}
