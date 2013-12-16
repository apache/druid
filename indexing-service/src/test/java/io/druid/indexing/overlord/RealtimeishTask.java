/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.LockReleaseAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;

import java.util.List;

/**
 */
public class RealtimeishTask extends AbstractTask
{
  public RealtimeishTask()
  {
    super("rt1", "rt", new TaskResource("rt1", 1), "foo");
  }

  public RealtimeishTask(String id, String groupId, TaskResource taskResource, String dataSource)
  {
    super(id, groupId, taskResource, dataSource);
  }

  @Override
  public String getType()
  {
    return "realtime_test";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final Interval interval1 = new Interval("2010-01-01T00/PT1H");
    final Interval interval2 = new Interval("2010-01-01T01/PT1H");

    // Sort of similar to what realtime tasks do:

    // Acquire lock for first interval
    final TaskLock lock1 = toolbox.getTaskActionClient().submit(new LockAcquireAction(interval1));
    final List<TaskLock> locks1 = toolbox.getTaskActionClient().submit(new LockListAction());

    // (Confirm lock sanity)
    Assert.assertEquals("lock1 interval", interval1, lock1.getInterval());
    Assert.assertEquals("locks1", ImmutableList.of(lock1), locks1);

    // Acquire lock for second interval
    final TaskLock lock2 = toolbox.getTaskActionClient().submit(new LockAcquireAction(interval2));
    final List<TaskLock> locks2 = toolbox.getTaskActionClient().submit(new LockListAction());

    // (Confirm lock sanity)
    Assert.assertEquals("lock2 interval", interval2, lock2.getInterval());
    Assert.assertEquals("locks2", ImmutableList.of(lock1, lock2), locks2);

    // Push first segment
    toolbox.getTaskActionClient()
           .submit(
               new SegmentInsertAction(
                   ImmutableSet.of(
                       DataSegment.builder()
                                  .dataSource("foo")
                                  .interval(interval1)
                                  .version(lock1.getVersion())
                                  .build()
                   )
               )
           );

    // Release first lock
    toolbox.getTaskActionClient().submit(new LockReleaseAction(interval1));
    final List<TaskLock> locks3 = toolbox.getTaskActionClient().submit(new LockListAction());

    // (Confirm lock sanity)
    Assert.assertEquals("locks3", ImmutableList.of(lock2), locks3);

    // Push second segment
    toolbox.getTaskActionClient()
           .submit(
               new SegmentInsertAction(
                   ImmutableSet.of(
                       DataSegment.builder()
                                  .dataSource("foo")
                                  .interval(interval2)
                                  .version(lock2.getVersion())
                                  .build()
                   )
               )
           );

    // Release second lock
    toolbox.getTaskActionClient().submit(new LockReleaseAction(interval2));
    final List<TaskLock> locks4 = toolbox.getTaskActionClient().submit(new LockListAction());

    // (Confirm lock sanity)
    Assert.assertEquals("locks4", ImmutableList.<TaskLock>of(), locks4);

    // Exit
    return TaskStatus.success(getId());
  }
}
