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

package org.apache.druid.indexing.overlord;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.LockReleaseAction;
import org.apache.druid.indexing.common.actions.SegmentInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;

import java.util.List;

/**
 */
public class RealtimeishTask extends AbstractTask
{
  public RealtimeishTask()
  {
    super("rt1", "rt", new TaskResource("rt1", 1), "foo", null);
  }

  @Override
  public String getType()
  {
    return "realtime_test";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final Interval interval1 = Intervals.of("2010-01-01T00/PT1H");
    final Interval interval2 = Intervals.of("2010-01-01T01/PT1H");

    // Sort of similar to what realtime tasks do:

    // Acquire lock for first interval
    final TaskLock lock1 = toolbox.getTaskActionClient().submit(
        new TimeChunkLockAcquireAction(TaskLockType.EXCLUSIVE, interval1, 5000)
    );
    Assert.assertNotNull(lock1);
    final List<TaskLock> locks1 = toolbox.getTaskActionClient().submit(new LockListAction());

    // (Confirm lock sanity)
    Assert.assertEquals("lock1 interval", interval1, lock1.getInterval());
    Assert.assertEquals("locks1", ImmutableList.of(lock1), locks1);

    // Acquire lock for second interval
    final TaskLock lock2 = toolbox.getTaskActionClient().submit(
        new TimeChunkLockAcquireAction(TaskLockType.EXCLUSIVE, interval2, 5000)
    );
    Assert.assertNotNull(lock2);
    final List<TaskLock> locks2 = toolbox.getTaskActionClient().submit(new LockListAction());

    // (Confirm lock sanity)
    Assert.assertEquals("lock2 interval", interval2, lock2.getInterval());
    Assert.assertEquals("locks2", ImmutableList.of(lock1, lock2), locks2);

    // Push first segment
    SegmentInsertAction firstSegmentInsertAction = new SegmentInsertAction(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource("foo")
                       .interval(interval1)
                       .version(lock1.getVersion())
                       .build()
        )
    );
    toolbox.getTaskActionClient().submit(firstSegmentInsertAction);

    // Release first lock
    toolbox.getTaskActionClient().submit(new LockReleaseAction(interval1));
    final List<TaskLock> locks3 = toolbox.getTaskActionClient().submit(new LockListAction());

    // (Confirm lock sanity)
    Assert.assertEquals("locks3", ImmutableList.of(lock2), locks3);

    // Push second segment
    SegmentInsertAction secondSegmentInsertAction = new SegmentInsertAction(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource("foo")
                       .interval(interval2)
                       .version(lock2.getVersion())
                       .build()
        )
    );
    toolbox.getTaskActionClient().submit(secondSegmentInsertAction);

    // Release second lock
    toolbox.getTaskActionClient().submit(new LockReleaseAction(interval2));
    final List<TaskLock> locks4 = toolbox.getTaskActionClient().submit(new LockListAction());

    // (Confirm lock sanity)
    Assert.assertEquals("locks4", ImmutableList.<TaskLock>of(), locks4);

    // Exit
    return TaskStatus.success(getId());
  }
}
