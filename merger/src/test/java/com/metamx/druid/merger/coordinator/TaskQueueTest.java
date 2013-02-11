/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.task.AbstractTask;
import com.metamx.druid.merger.common.task.Task;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class TaskQueueTest
{
  @Test
  public void testEmptyQueue() throws Exception
  {
    final TaskQueue tq = newTaskQueue();

    // get task status for nonexistent task
    Assert.assertFalse("getStatus", tq.getStatus("foo").isPresent());

    // poll on empty queue
    Assert.assertNull("poll", tq.poll());
  }

  private static TaskQueue newTaskQueue()
  {
    return newTaskQueueWithStorage(new LocalTaskStorage());
  }

  public static TaskQueue newTaskQueueWithStorage(TaskStorage storage)
  {
    final TaskQueue tq = new TaskQueue(storage);
    tq.start();
    return tq;
  }

  @Test
  public void testAddRemove() throws Exception
  {
    final TaskQueue tq = newTaskQueue();
    final Task[] tasks = {
        newTask("T0", "G0", "bar", new Interval("2011/P1Y")),
        newTask("T1", "G1", "bar", new Interval("2011-03-01/P1D")),
        newTask("T2", "G2", "foo", new Interval("2011-03-01/P1D")),
        newTask("T3", "G3", "foo", new Interval("2011/P1Y")),
        newTask("T4", "G4", "foo", new Interval("2012-01-02/P1D")),
        newTask("T5", "G5", "foo", new Interval("2012-02-01/PT1H"))
    };

    Throwable thrown;

    for(Task task : tasks) {
      tq.add(task);
    }

    // get task status for in-progress task
    Assert.assertEquals("T2 status (before finishing)", TaskStatus.Status.RUNNING, tq.getStatus(tasks[2].getId()).get().getStatusCode());

    // Can't add tasks with the same id
    thrown = null;
    try {
      tq.add(newTask("T5", "G5", "baz", new Interval("2013-02-01/PT1H")));
    } catch(IllegalStateException e) {
      thrown = e;
    }

    Assert.assertNotNull("Exception on duplicate task id", thrown);

    // take max number of tasks
    final List<Task> taken = Lists.newArrayList();
    while (true) {
      final VersionedTaskWrapper taskWrapper = tq.poll();
      if(taskWrapper != null) {
        taken.add(taskWrapper.getTask());
      } else {
        break;
      }
    }

    // check them
    Assert.assertEquals(
        "Taken tasks (round 1)",
        Lists.newArrayList(
            tasks[0], tasks[2], tasks[4], tasks[5]
        ),
        taken
    );

    // mark one done
    tq.done(tasks[2], tasks[2].run(null, null));

    // get its status back
    Assert.assertEquals("T2 status (after finishing)", TaskStatus.Status.SUCCESS, tq.getStatus(tasks[2].getId()).get().getStatusCode());

    // Can't do a task twice
    thrown = null;
    try {
      tq.done(tasks[2], tasks[2].run(null, null));
    } catch(IllegalStateException e) {
      thrown = e;
    }

    Assert.assertNotNull("Exception on twice-done task", thrown);

    // we should be able to get one more task now
    taken.clear();
    while (true) {
      final VersionedTaskWrapper taskWrapper = tq.poll();
      if(taskWrapper != null) {
        taken.add(taskWrapper.getTask());
      } else {
        break;
      }
    }

    // check it
    Assert.assertEquals(
        "Taken tasks (round 2)",
        Lists.newArrayList(
            tasks[3]
        ),
        taken
    );

    // there should be no more tasks to get
    Assert.assertNull("poll queue with no tasks available", tq.poll());
  }

  @Test
  public void testContinues() throws Exception
  {
    final TaskQueue tq = newTaskQueue();

    final Task t0 = newTask("T0", "G0", "bar", new Interval("2011/P1Y"));
    final Task t1 = newContinuedTask("T1", "G1", "bar", new Interval("2013/P1Y"), Lists.newArrayList(t0));
    tq.add(t1);

    Assert.assertTrue("T0 isPresent (#1)",  !tq.getStatus("T0").isPresent());
    Assert.assertTrue("T1 isPresent (#1)",   tq.getStatus("T1").isPresent());
    Assert.assertTrue("T1 isRunnable (#1)",  tq.getStatus("T1").get().isRunnable());
    Assert.assertTrue("T1 isComplete (#1)", !tq.getStatus("T1").get().isComplete());

    // should be able to get t1 out
    Assert.assertEquals("poll #1", "T1", tq.poll().getTask().getId());
    Assert.assertNull("poll #2", tq.poll());

    // report T1 done. Should cause T0 to be created
    tq.done(t1, t1.run(null, null));

    Assert.assertTrue("T0 isPresent (#2)",   tq.getStatus("T0").isPresent());
    Assert.assertTrue("T0 isRunnable (#2)",  tq.getStatus("T0").get().isRunnable());
    Assert.assertTrue("T0 isComplete (#2)", !tq.getStatus("T0").get().isComplete());
    Assert.assertTrue("T1 isPresent (#2)",   tq.getStatus("T1").isPresent());
    Assert.assertTrue("T1 isRunnable (#2)", !tq.getStatus("T1").get().isRunnable());
    Assert.assertTrue("T1 isComplete (#2)",  tq.getStatus("T1").get().isComplete());

    // should be able to get t0 out
    Assert.assertEquals("poll #3", "T0", tq.poll().getTask().getId());
    Assert.assertNull("poll #4", tq.poll());

    // report T0 done. Should cause T0, T1 to be marked complete
    tq.done(t0, t0.run(null, null));

    Assert.assertTrue("T0 isPresent (#3)",   tq.getStatus("T0").isPresent());
    Assert.assertTrue("T0 isRunnable (#3)", !tq.getStatus("T0").get().isRunnable());
    Assert.assertTrue("T0 isComplete (#3)",  tq.getStatus("T0").get().isComplete());
    Assert.assertTrue("T1 isPresent (#3)",   tq.getStatus("T1").isPresent());
    Assert.assertTrue("T1 isRunnable (#3)", !tq.getStatus("T1").get().isRunnable());
    Assert.assertTrue("T1 isComplete (#3)",  tq.getStatus("T1").get().isComplete());

    // should be no more events available for polling
    Assert.assertNull("poll #5", tq.poll());
  }

  @Test
  public void testConcurrency() throws Exception
  {
    final TaskQueue tq = newTaskQueue();

    // Imagine a larger task that splits itself up into pieces
    final Task t1 = newTask("T1", "G0", "bar", new Interval("2011-01-01/P1D"));
    final Task t2 = newTask("T2", "G1", "bar", new Interval("2011-01-02/P1D")); // Task group different from original
    final Task t3 = newTask("T3", "G0", "bar", new Interval("2011-01-03/P1D"));
    final Task t4 = newTask("T4", "G0", "bar", new Interval("2011-01-02/P5D")); // Interval wider than original
    final Task t0 = newContinuedTask(
        "T0",
        "G0",
        "bar",
        new Interval("2011-01-01/P3D"),
        ImmutableList.of(t1, t2, t3, t4)
    );

    tq.add(t0);

    final VersionedTaskWrapper wt0 = tq.poll();
    Assert.assertEquals("wt0 task id", "T0", wt0.getTask().getId());
    Assert.assertNull("null poll #1", tq.poll());

    // Sleep a bit to avoid false test passes
    Thread.sleep(5);

    // Finish t0
    tq.done(t0, t0.run(null, null));

    // take max number of tasks
    final Set<String> taken = Sets.newHashSet();
    while (true) {

      // Sleep a bit to avoid false test passes
      Thread.sleep(5);

      final VersionedTaskWrapper taskWrapper = tq.poll();

      if(taskWrapper != null) {
        Assert.assertEquals(
            String.format("%s version", taskWrapper.getTask().getId()),
            wt0.getVersion(),
            taskWrapper.getVersion()
        );
        taken.add(taskWrapper.getTask().getId());
      } else {
        break;
      }

    }

    Assert.assertEquals("taken", Sets.newHashSet("T1", "T3"), taken);

    // Finish t1
    tq.done(t1, t1.run(null, null));
    Assert.assertNull("null poll #2", tq.poll());

    // Finish t3
    tq.done(t3, t3.run(null, null));

    // We should be able to get t2 now
    final VersionedTaskWrapper wt2 = tq.poll();
    Assert.assertEquals("wt2 task id", "T2", wt2.getTask().getId());
    Assert.assertEquals("wt2 group id", "G1", wt2.getTask().getGroupId());
    Assert.assertNotSame("wt2 version", wt0.getVersion(), wt2.getVersion());
    Assert.assertNull("null poll #3", tq.poll());

    // Finish t2
    tq.done(t2, t2.run(null, null));

    // We should be able to get t4
    // And it should be in group G0, but that group should have a different version than last time
    // (Since the previous transaction named "G0" has ended and transaction names are not necessarily tied to
    // one version if they end and are re-started)
    final VersionedTaskWrapper wt4 = tq.poll();
    Assert.assertEquals("wt4 task id", "T4", wt4.getTask().getId());
    Assert.assertEquals("wt4 group id", "G0", wt4.getTask().getGroupId());
    Assert.assertNotSame("wt4 version", wt0.getVersion(), wt4.getVersion());
    Assert.assertNotSame("wt4 version", wt2.getVersion(), wt4.getVersion());

    // Kind of done testing at this point, but let's finish t4 anyway
    tq.done(t4, t4.run(null, null));
    Assert.assertNull("null poll #4", tq.poll());
  }

  @Test
  public void testBootstrap() throws Exception
  {
    final TaskStorage storage = new LocalTaskStorage();
    storage.insert(newTask("T1", "G1", "bar", new Interval("2011-01-01/P1D")), TaskStatus.running("T1"));
    storage.insert(newTask("T2", "G2", "bar", new Interval("2011-02-01/P1D")), TaskStatus.running("T2"));
    storage.setVersion("T1", "1234");

    final TaskQueue tq = newTaskQueueWithStorage(storage);

    final VersionedTaskWrapper vt1 = tq.poll();
    Assert.assertEquals("vt1 id", "T1", vt1.getTask().getId());
    Assert.assertEquals("vt1 version", "1234", vt1.getVersion());

    tq.done(vt1.getTask(), TaskStatus.success("T1", ImmutableList.<DataSegment>of()));

    // re-bootstrap
    tq.stop();
    storage.setStatus("T2", TaskStatus.failure("T2"));
    tq.start();

    Assert.assertNull("null poll", tq.poll());
  }

  private static Task newTask(final String id, final String groupId, final String dataSource, final Interval interval)
  {
    return new AbstractTask(id, groupId, dataSource, interval)
    {
      @Override
      public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception
      {
        return TaskStatus.success(
            id,
            Lists.newArrayList(
                new DataSegment(
                    dataSource,
                    interval,
                    new DateTime("2012-01-02").toString(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    -1
                )
            )
        );
      }

      @Override
      public Type getType()
      {
        return Type.TEST;
      }
    };
  }

  private static Task newContinuedTask(
      final String id,
      final String groupId,
      final String dataSource,
      final Interval interval,
      final List<Task> nextTasks
  )
  {
    return new AbstractTask(id, groupId, dataSource, interval)
    {
      @Override
      public Type getType()
      {
        return Type.TEST;
      }

      @Override
      public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception
      {
        return TaskStatus.continued(id, nextTasks);
      }
    };
  }
}
