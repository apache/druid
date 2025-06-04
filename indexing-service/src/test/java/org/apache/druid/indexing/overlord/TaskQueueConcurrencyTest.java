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
import java.util.List;
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
  private TaskQueue taskQueue;

  private Map<String, UpdateAction> threadToUpdateAction;

  @Override
  public void setUpIngestionTestBase() throws IOException
  {
    super.setUpIngestionTestBase();

    threadToUpdateAction = new HashMap<>();

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
        // Override this critical action so that we can track and control progress
        final String threadName = Thread.currentThread().getName();
        final UpdateAction updateAction = threadToUpdateAction.remove(threadName);

        return updateAction == null
               ? super.addOrUpdateTaskEntry(taskId, updateOperation)
               : super.addOrUpdateTaskEntry(
                   taskId,
                   existing -> updateAction.critical.perform(() -> updateOperation.apply(existing))
               );
      }

      @Override
      void setActive(boolean active)
      {
        // Override this critical action so that we can track and control progress
        final String threadName = Thread.currentThread().getName();
        final UpdateAction updateAction = threadToUpdateAction.remove(threadName);

        if (updateAction == null) {
          super.setActive(active);
        } else {
          updateAction.critical.perform(() -> {
            super.setActive(active);
            return 0;
          });
        }
      }
    };
  }

  @Test(timeout = 20_000L)
  public void test_start_blocks_add_forAnyTaskId()
  {
    // Add task1 to storage and mark it as running
    final Task task1 = createTask("t1");
    getTaskStorage().insert(task1, TaskStatus.running(task1.getId()));

    final Task task2 = createTask("t2");

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.start()
        ).withEndState(
            () -> Assert.assertEquals(List.of(task1), taskQueue.getTasks())
        )
    ).blocks(
        update(
            () -> taskQueue.add(task2)
        ).withEndState(
            () -> Assert.assertEquals(List.of(task1, task2), taskQueue.getTasks())
        )
    );
  }

  @Test(timeout = 20_000L)
  public void test_add_blocks_stop()
  {
    taskQueue.setActive(true);

    final Task task = createTask("t1");
    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.add(task)
        ).withEndState(
            () -> Assert.assertEquals(Optional.of(task), taskQueue.getActiveTask(task.getId()))
        )
    ).blocks(
        update(
            () -> taskQueue.stop()
        ).withEndState(
            () -> Assert.assertEquals(Optional.absent(), taskQueue.getActiveTask(task.getId()))
        )
    );
  }

  @Test(timeout = 20_000L)
  public void test_add_blocks_syncFromStorage_forSameTaskId()
  {
    taskQueue.setActive(true);

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

  @Test(timeout = 20_000L)
  public void test_syncFromStorage_blocks_add_forSameTaskId()
  {
    final String taskId = "t2";
    final Task task = createTask(taskId);

    // Add the task to queue and storage
    taskQueue.setActive(true);
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

    taskQueue.setActive(true);
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
    taskQueue.setActive(true);

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
    taskQueue.setActive(true);

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

  @Test(timeout = 20_000L)
  public void test_add_doesNotBlock_add_forDifferentTaskId()
  {
    taskQueue.setActive(true);

    final Task task1 = createTask("t1");
    final Task task2 = createTask("t2");

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.add(task1)
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(task1),
                taskQueue.getActiveTask(task1.getId())
            )
        )
    ).doesNotBlock(
        update(
            () -> taskQueue.add(task2)
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(task2),
                taskQueue.getActiveTask(task2.getId())
            )
        )
    );
  }

  @Test(timeout = 20_000L)
  public void test_add_doesNotBlock_shutdown_forDifferentTaskId()
  {
    taskQueue.setActive(true);

    final Task task1 = createTask("t1");
    taskQueue.add(task1);

    final Task task2 = createTask("t2");

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.add(task2)
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(task2),
                taskQueue.getActiveTask(task2.getId())
            )
        )
    ).doesNotBlock(
        update(
            () -> taskQueue.shutdown(task1.getId(), "killed")
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(TaskStatus.failure(task1.getId(), "killed")),
                taskQueue.getTaskStatus(task1.getId())
            )
        )
    );
  }

  @Test(timeout = 20_000L)
  public void test_shutdown_doesNotBlock_add_forDifferentTaskId()
  {
    taskQueue.setActive(true);

    final Task task1 = createTask("t1");
    taskQueue.add(task1);

    final Task task2 = createTask("t2");

    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.shutdown(task1.getId(), "killed")
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(TaskStatus.failure(task1.getId(), "killed")),
                taskQueue.getTaskStatus(task1.getId())
            )
        )
    ).doesNotBlock(
        update(
            () -> taskQueue.add(task2)
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(task2),
                taskQueue.getActiveTask(task2.getId())
            )
        )
    );
  }

  @Test(timeout = 20_000L)
  public void test_shutdown_then_manageQueuedTasks_blocks_syncFromStorage_and_forcesTaskRemoval()
  {
    taskQueue.setActive(true);

    final Task task1 = createTask("t1");
    taskQueue.add(task1);

    // shutdown the task ahead of time to mark it as isComplete
    taskQueue.shutdown(task1.getId(), "shutdown");

    // verify that managedQueuedTasks() called before syncFromStorage() forces the sync to block
    // but ensures that syncFromStorage() is able to remove the task
    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.manageQueuedTasks()
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.of(TaskStatus.failure(task1.getId(), "shutdown")),
                taskQueue.getTaskStatus(task1.getId())
            )
        )
    ).blocks(
        update(
            () -> taskQueue.syncFromStorage()
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.absent(),
                taskQueue.getActiveTask(task1.getId())
            )
        )
    );

    Assert.assertEquals(Optional.absent(), taskQueue.getActiveTask(task1.getId()));
  }

  @Test(timeout = 20_000L)
  public void test_shutdown_then_syncFromStorage_blocks_manageQueuedTasks_and_forcesTaskRemoval()
  {
    taskQueue.setActive(true);

    final Task task1 = createTask("t1");
    taskQueue.add(task1);

    // shutdown the task ahead of time to mark it as isComplete
    taskQueue.shutdown(task1.getId(), "shutdown");

    // verify that syncFromStorage() called before managedQueuedTasks() forces the sync to block
    // but ensures that syncFromStorage() is able to remove the task
    ActionVerifier.verifyThat(
        update(
            () -> taskQueue.syncFromStorage()
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.absent(),
                taskQueue.getActiveTask(task1.getId())
            )
        )
    ).blocks(
        update(
            () -> taskQueue.manageQueuedTasks()
        ).withEndState(
            () -> Assert.assertEquals(
                Optional.absent(),
                taskQueue.getActiveTask(task1.getId())
            )
        )
    );

    Assert.assertEquals(Optional.absent(), taskQueue.getActiveTask(task1.getId()));
  }

  private UpdateAction update(Action action)
  {
    return new UpdateAction(action, threadToUpdateAction::put);
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

  @FunctionalInterface
  private interface UpdateStartNotifier
  {
    void onUpdateStart(String threadName, UpdateAction action);
  }

  /**
   * Wrapper around the critical part of an update action.
   * This class contains latches to track and control the progress of the update
   * and verify behaviour in race conditions.
   */
  private static class CriticalUpdate
  {
    final CountDownLatch isReadyToStart = new NamedLatch("isReadyToStart");
    final CountDownLatch start = new NamedLatch("start");

    final CountDownLatch isReadyToFinish = new NamedLatch("isReadyToFinish");
    final CountDownLatch finish = new NamedLatch("finish");

    synchronized <V> V perform(Supplier<V> updateComputation)
    {
      if (isReadyToStart.getCount() == 0) {
        throw new ISE("Critical update has already run on another thread");
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
    final UpdateStartNotifier startNotifier;

    Action verifyAction;

    UpdateAction(Action action, UpdateStartNotifier startNotifier)
    {
      this.action = action;
      this.startNotifier = startNotifier;
    }

    UpdateAction withEndState(Action verifyAction)
    {
      this.verifyAction = verifyAction;
      return this;
    }

    void perform()
    {
      startNotifier.onUpdateStart(Thread.currentThread().getName(), this);

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
     * Verifies that the critical part of {@code update1} completely blocks the
     * critical part of {@code update2}.
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

      // Finish update 1 critical and verify that update 2 is now ready to start
      update1.critical.finish.countDown();
      waitFor(update2.critical.isReadyToStart);

      // Finish update 1
      update1.waitToFinishAndVerify();

      // Start and finish update2
      update2.critical.start.countDown();
      waitFor(update2.critical.isReadyToFinish);
      update2.critical.finish.countDown();
      update2.waitToFinishAndVerify();

      executor.shutdownNow();
    }

    /**
     * Verifies that the critical part of {@code update1} does not
     * block the critical part of {@code update2}.
     */
    void doesNotBlock(UpdateAction update2)
    {
      final ExecutorService executor = Execs.multiThreaded(2, "TaskQueueConcurrencyTest-%s");

      // Start update 1 and wait for it to enter critical section
      executor.submit(update1::perform);
      waitFor(update1.critical.isReadyToStart);

      // Start update2 and verify that it has also entered critical section
      executor.submit(update2::perform);
      waitFor(update2.critical.isReadyToStart);

      // Finish update2 to prove that it is not blocked by update1
      update2.critical.start.countDown();
      waitFor(update2.critical.isReadyToFinish);
      update2.critical.finish.countDown();
      update2.waitToFinishAndVerify();

      // Start and finish update1
      update1.critical.start.countDown();
      waitFor(update1.critical.isReadyToFinish);
      update1.critical.finish.countDown();
      update1.waitToFinishAndVerify();

      executor.shutdownNow();
    }
  }
}
