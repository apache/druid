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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.IngestionTestBase;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.NoopTaskContextEnricher;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Unit tests to verify that operations performed on {@link TaskQueue} for a
 * given task ID are thread-safe, so that the {@link TaskQueue} always has a
 * consistent view of the status of a task.
 */
public class TaskQueueConcurrencyTest extends IngestionTestBase
{
  private static final String THREAD_NAME_FORMAT = "TaskQueueConcurrencyTest-%s";

  private TaskQueue taskQueue;

  private int threadId;
  private Map<String, CriticalUpdate> threadToCriticalUpdate;

  @Override
  public void setUpIngestionTestBase() throws IOException
  {
    super.setUpIngestionTestBase();

    threadId = 0;
    threadToCriticalUpdate = new HashMap<>();

    taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(3, null, null, null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        new TaskQueueTest.SimpleTaskRunner(NoopServiceEmitter.instance())
        {
          @Override
          public ListenableFuture<TaskStatus> run(Task task)
          {
            return Futures.immediateFuture(TaskStatus.success(task.getId()));
          }
        },
        createActionClientFactory(),
        getLockbox(),
        NoopServiceEmitter.instance(),
        getObjectMapper(),
        new NoopTaskContextEnricher()
    )
    {
      @Override
      TaskEntry addOrUpdateTaskEntry(String taskId, Function<TaskEntry, TaskEntry> updateOperation)
      {
        // Use a modified update for this thread
        final String threadName = Thread.currentThread().getName();
        final CriticalUpdate criticalUpdate = threadToCriticalUpdate.get(threadName);

        return criticalUpdate == null
               ? super.addOrUpdateTaskEntry(taskId, updateOperation)
               : super.addOrUpdateTaskEntry(
                   taskId,
                   existing -> criticalUpdate.perform(() -> updateOperation.apply(existing))
               );
      }
    };

    taskQueue.setActive();
  }

  @Test
  public void test_add_blocks_syncFromStorage_forSameTaskId()
  {
    final String taskId = "t2";
    final Task task = createTask(taskId);

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.add(task)
        ).withEndState(
            () -> Assert.assertEquals(Optional.of(task), taskQueue.getActiveTask(taskId))
        )
    ).blocks(
        update(
            () -> taskQueue.syncFromStorage()
        ).withEndState(
            () -> Assert.assertEquals(Optional.of(task), taskQueue.getActiveTask(taskId))
        )
    );
  }

  @Test
  public void test_syncFromStorage_blocks_add_forSameTaskId()
  {
    final String taskId = "t2";
    final Task task = createTask(taskId);

    // Add the task to queue and storage
    taskQueue.add(task);
    Assert.assertEquals(Optional.of(task), taskQueue.getActiveTask(taskId));
    Assert.assertEquals(Optional.of(task), getTaskStorage().getTask(taskId));

    // Mark the task as completed and remove it from storage but not queue
    taskQueue.shutdown(taskId, "test");
    getTaskStorage().removeTasksOlderThan(DateTimes.nowUtc().plusDays(1).getMillis());

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.syncFromStorage()
        ).withEndState(
            () -> Assert.assertEquals(Optional.absent(), taskQueue.getActiveTask(taskId))
        )
    ).blocks(
        update(
            () -> taskQueue.add(task)
        ).withEndState(
            () -> Assert.assertEquals(Optional.of(task), taskQueue.getActiveTask(taskId))
        )
    );
  }

  @Test(timeout = 20_000L)
  public void test_manageQueuedTasks_blocks_shutdown_forSameTaskId()
  {
    final String taskId = "t2";
    final Task task = createTask(taskId);

    taskQueue.add(task);
    Assert.assertEquals(Optional.of(task), taskQueue.getActiveTask(taskId));

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.manageQueuedTasks()
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(TaskStatus.running(taskId)),
                taskQueue.getTaskStatus(taskId)
            )
        )
    ).blocks(
        update(
            () -> taskQueue.shutdown(taskId, "test")
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(TaskStatus.failure(taskId, "test")),
                taskQueue.getTaskStatus(taskId)
            )
        )
    );
  }

  @Test(timeout = 20_000L)
  public void test_add_blocks_shutdownWithSuccess_forSameTaskId()
  {
    final String taskId = "t2";
    final Task task = createTask(taskId);

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.add(task)
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(task),
                taskQueue.getActiveTask(taskId)
            )
        )
    ).blocks(
        update(
            () -> taskQueue.shutdownWithSuccess(task.getId(), "test")
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(TaskStatus.success(taskId)),
                taskQueue.getTaskStatus(taskId)
            )
        )
    );
  }

  @Test(timeout = 20_000L)
  public void test_add_blocks_shutdown_forSameTaskId()
  {
    final String taskId = "t1";
    final Task task = createTask(taskId);

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.add(task)
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(task),
                taskQueue.getActiveTask(taskId)
            )
        )
    ).blocks(
        update(
            () -> taskQueue.shutdown(task.getId(), "test")
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(TaskStatus.failure(taskId, "test")),
                taskQueue.getTaskStatus(taskId)
            )
        )
    );
  }

  private UpdateAction update(Action action)
  {
    final UpdateAction updateAction = new UpdateAction(action);
    threadToCriticalUpdate.put(StringUtils.format(THREAD_NAME_FORMAT, threadId++), updateAction.critical);

    return updateAction;
  }

  private static Task createTask(String id)
  {
    return new NoopTask(id, id, TestDataSource.WIKI, 0L, 0L, Map.of());
  }

  private static void waitFor(CountDownLatch latch)
  {
    try {
      boolean done = latch.await(5, TimeUnit.SECONDS);
      Assert.assertTrue(StringUtils.format("Latch[%s] is still blocked", latch), done);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  private interface Action
  {
    void perform();
  }

  /**
   * Wrapper around a critical update action on a task in the {@link TaskQueue}.
   * This class is used to identify the current state of the update and pause or
   * resume it to force certain race conditions.
   */
  private static class CriticalUpdate
  {
    final CountDownLatch isReadyToStart = new NamedLatch("hasStarted");
    final CountDownLatch start = new NamedLatch("resume");

    final CountDownLatch isReadyToFinish = new NamedLatch("isReadyToFinish");
    final CountDownLatch finish = new NamedLatch("finish");

    synchronized <V> V perform(Supplier<V> updateComputation)
    {
      if (isReadyToStart.getCount() == 0) {
        throw new ISE("Update has already run on another thread");
      }

      isReadyToStart.countDown();
      waitFor(start);

      // Actual update operation
      final V updatedValue = updateComputation.get();

      isReadyToFinish.countDown();
      waitFor(finish);

      return updatedValue;
    }
  }

  /**
   * CountDownLatch with a name to easily identify bugs.
   */
  private static class NamedLatch extends CountDownLatch
  {
    final String name;

    private NamedLatch(String name)
    {
      super(1);
      this.name = name;
    }

    @Override
    public String toString()
    {
      return name;
    }
  }

  /**
   * An update action with a critical part and a verification step.
   */
  private static class UpdateAction
  {
    final CountDownLatch finished = new NamedLatch("finished");
    final CriticalUpdate critical = new CriticalUpdate();

    final Action action;
    Action verifyAction;

    UpdateAction(Action action)
    {
      this.action = action;
    }

    UpdateAction withEndState(Action verifyAction)
    {
      this.verifyAction = verifyAction;
      return this;
    }

    void perform()
    {
      try {
        action.perform();
        finished.countDown();
      }
      catch (Throwable t) {
        t.printStackTrace(System.out);
      }
    }

    void waitToFinishAndVerify()
    {
      waitFor(finished);
      verifyAction.perform();
    }
  }

  /**
   * Verifies thread-safety between two actions.
   */
  private static class ActionVerifier
  {
    UpdateAction update1;

    static ActionVerifier verifyThat(UpdateAction update1)
    {
      ActionVerifier verifier = new ActionVerifier();
      verifier.update1 = update1;
      return verifier;
    }

    /**
     * Verifies that the critical update section of {@code operation1} completely
     * blocks the critical update in {@code operation2}.
     */
    void blocks(UpdateAction update2)
    {
      final ExecutorService executor = Execs.multiThreaded(2, "TaskQueueConcurrencyTest-%s");

      // Start update 1 and wait for it to enter critical section
      executor.submit(update1::perform);
      waitFor(update1.critical.isReadyToStart);

      executor.submit(update2::perform);

      // Wait for some time and verify that update 2 critical has not started yet
      try {
        Thread.sleep(1000);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      Assert.assertEquals(1, update2.critical.isReadyToStart.getCount());

      update1.critical.start.countDown();

      // Wait for update 1 critical to reach finish
      // and verify that update 2 critical has not started yet
      waitFor(update1.critical.isReadyToFinish);
      Assert.assertEquals(1, update2.critical.isReadyToStart.getCount());

      // Finish update 1 and verify that update 2 critical has now started
      update1.critical.finish.countDown();
      waitFor(update2.critical.isReadyToStart);

      update1.waitToFinishAndVerify();

      update2.critical.start.countDown();
      waitFor(update2.critical.isReadyToFinish);
      update2.critical.finish.countDown();

      update2.waitToFinishAndVerify();

      executor.shutdownNow();
    }
  }
}
