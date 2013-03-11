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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.emitter.EmittingLogger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Interface between task producers and task consumers.
 * <p/>
 * The queue accepts tasks from producers using {@link #add} and delivers tasks to consumers using either
 * {@link #take} or {@link #poll}. Ordering is mostly-FIFO, with deviations when the natural next task would conflict
 * with a currently-running task. In that case, tasks are skipped until a runnable one is found.
 * <p/>
 * To manage locking, the queue keeps track of currently-running tasks as {@link com.metamx.druid.merger.common.TaskLock} objects. The idea is that
 * only one TaskLock can be running on a particular dataSource + interval, and that TaskLock has a single version
 * string that all tasks in the group must use to publish segments. Tasks in the same TaskLock may run concurrently.
 * <p/>
 * For persistence, the queue saves new tasks from {@link #add} and task status updates from {@link #notify} using a
 * {@link TaskStorage} obj
 * <p/>
 * To support leader election of our containing system, the queue can be stopped (in which case it will not accept
 * any new tasks, or hand out any more tasks, until started again).
 */
public class TaskQueue
{
  private final List<Task> queue = Lists.newLinkedList();
  private final TaskStorage taskStorage;
  private final TaskLockbox taskLockbox;
  private final ReentrantLock giant = new ReentrantLock();
  private final Condition workMayBeAvailable = giant.newCondition();

  private volatile boolean active = false;

  private static final EmittingLogger log = new EmittingLogger(TaskQueue.class);

  public TaskQueue(TaskStorage taskStorage, TaskLockbox taskLockbox)
  {
    this.taskStorage = Preconditions.checkNotNull(taskStorage, "taskStorage");
    this.taskLockbox = Preconditions.checkNotNull(taskLockbox, "taskLockbox");
  }

  /**
   * Bootstraps this task queue and associated task lockbox. Clears the lockbox before running. Should be called
   * while the queue is stopped. It is not a good idea to start the queue if this method fails.
   */
  public void bootstrap()
  {
    giant.lock();

    try {
      Preconditions.checkState(!active, "queue must be stopped");

      log.info("Bootstrapping queue (and associated lockbox)");

      queue.clear();
      taskLockbox.clear();

      // Add running tasks to the queue
      final List<Task> runningTasks = taskStorage.getRunningTasks();

      for(final Task task : runningTasks) {
        queue.add(task);
      }

      // Get all locks, along with which tasks they belong to
      final Multimap<TaskLock, Task> tasksByLock = ArrayListMultimap.create();
      for(final Task runningTask : runningTasks) {
        for(final TaskLock taskLock : taskStorage.getLocks(runningTask.getId())) {
          tasksByLock.put(taskLock, runningTask);
        }
      }

      // Sort locks by version
      final Ordering<Map.Entry<TaskLock, Task>> byVersionOrdering = new Ordering<Map.Entry<TaskLock, Task>>()
      {
        @Override
        public int compare(Map.Entry<TaskLock, Task> left, Map.Entry<TaskLock, Task> right)
        {
          return left.getKey().getVersion().compareTo(right.getKey().getVersion());
        }
      };

      // Acquire as many locks as possible, in version order
      for(final Map.Entry<TaskLock, Task> taskAndLock : byVersionOrdering.sortedCopy(tasksByLock.entries())) {
        final Task task = taskAndLock.getValue();
        final TaskLock savedTaskLock = taskAndLock.getKey();

        final Optional<TaskLock> acquiredTaskLock = taskLockbox.tryLock(
            task,
            savedTaskLock.getInterval(),
            Optional.of(savedTaskLock.getVersion())
        );

        if(acquiredTaskLock.isPresent() && savedTaskLock.getVersion().equals(acquiredTaskLock.get().getVersion())) {
          log.info(
              "Reacquired lock on interval[%s] version[%s] for task: %s",
              savedTaskLock.getInterval(),
              savedTaskLock.getVersion(),
              task.getId()
          );
        } else if(acquiredTaskLock.isPresent()) {
          log.info(
              "Could not reacquire lock on interval[%s] version[%s] (got version[%s] instead) for task: %s",
              savedTaskLock.getInterval(),
              savedTaskLock.getVersion(),
              acquiredTaskLock.get().getVersion(),
              task.getId()
          );
        } else {
          log.info(
              "Could not reacquire lock on interval[%s] version[%s] for task: %s",
              savedTaskLock.getInterval(),
              savedTaskLock.getVersion(),
              task.getId()
          );
        }
      }

      log.info("Bootstrapped %,d tasks. Ready to go!", runningTasks.size());
    } finally {
      giant.unlock();
    }
  }

  /**
   * Starts this task queue. Allows {@link #add(Task)} to accept new tasks. This should not be called on
   * an already-started queue.
   */
  @LifecycleStart
  public void start()
  {
    giant.lock();

    try {
      Preconditions.checkState(!active, "queue must be stopped");

      active = true;
      workMayBeAvailable.signalAll();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Shuts down the queue, for now. This may safely be called on an already-stopped queue. The queue may be restarted
   * if desired.
   */
  @LifecycleStop
  public void stop()
  {
    giant.lock();

    try {
      log.info("Naptime! Shutting down until we are started again.");
      queue.clear();
      taskLockbox.clear();
      active = false;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Adds some work to the queue and the underlying task storage facility with a generic "running" status.
   *
   * @param task task to add
   *
   * @return true
   */
  public boolean add(final Task task)
  {
    giant.lock();

    try {
      Preconditions.checkState(active, "Queue is not active!");

      // If this throws with any sort of exception, including TaskExistsException, we don't want to
      // insert the task into our queue.
      try {
        taskStorage.insert(task, TaskStatus.running(task.getId()));
      } catch(TaskExistsException e) {
        log.warn("Attempt to add task twice: %s", task.getId());
        throw Throwables.propagate(e);
      }

      queue.add(task);
      workMayBeAvailable.signalAll();

      // Attempt to add this task to a running task group. Silently continue if this is not possible.
      // The main reason this is here is so when subtasks are added, they end up in the same task group
      // as their parent whenever possible.
      if(task.getImplicitLockInterval().isPresent()) {
        taskLockbox.tryLock(task, task.getImplicitLockInterval().get());
      }

      return true;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Locks and returns next doable work from the queue. Blocks if there is no doable work.
   *
   * @return runnable task
   */
  public Task take() throws InterruptedException
  {
    giant.lock();

    try {
      Task task;

      log.info("Waiting for work...");

      while ((task = poll()) == null) {
        // awaitNanos because work may become available without this condition signalling,
        // due to other folks messing with the taskLockbox
        workMayBeAvailable.awaitNanos(1000000000L /* 1 second */);
      }

      return task;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Locks and removes next doable work from the queue. Returns null if there is no doable work.
   *
   * @return runnable task or null
   */
  public Task poll()
  {
    giant.lock();

    try {
      for (final Task task : queue) {
        if(task.getImplicitLockInterval().isPresent()) {
          // If this task has a fixed interval, attempt to lock it right now.
          final Optional<TaskLock> maybeLock = taskLockbox.tryLock(task, task.getImplicitLockInterval().get());
          if(maybeLock.isPresent()) {
            log.info("Task claimed with fixed interval lock: %s", task.getId());
            queue.remove(task);
            return task;
          }
        } else {
          // No fixed interval. Let's just run this and see what happens.
          log.info("Task claimed with no fixed interval lock: %s", task.getId());
          queue.remove(task);
          return task;
        }
      }

      return null;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Notify this queue that some task has an updated status. If this update is valid, the status will be persisted in
   * the task storage facility. If the status is a completed status, the task will be unlocked and no further
   * updates will be accepted.
   *
   * @param task           task to update
   * @param taskStatus new task status
   *
   * @throws NullPointerException     if task or status is null
   * @throws IllegalArgumentException if the task ID does not match the status ID
   * @throws IllegalStateException    if this queue is currently shut down
   */
  public void notify(final Task task, final TaskStatus taskStatus)
  {
    giant.lock();

    try {
      Preconditions.checkNotNull(task, "task");
      Preconditions.checkNotNull(taskStatus, "status");
      Preconditions.checkState(active, "Queue is not active!");
      Preconditions.checkArgument(
          task.getId().equals(taskStatus.getId()),
          "Mismatching task ids[%s/%s]",
          task.getId(),
          taskStatus.getId()
      );

      // Save status to DB
      boolean didPersistStatus = false;
      try {
        final Optional<TaskStatus> previousStatus = taskStorage.getStatus(task.getId());
        if (!previousStatus.isPresent() || !previousStatus.get().isRunnable()) {
          log.makeAlert("Ignoring notification for dead task").addData("task", task.getId()).emit();
          return;
        } else {
          taskStorage.setStatus(taskStatus);
          didPersistStatus = true;
        }
      } catch(Exception e) {
        log.makeAlert(e, "Failed to persist status for task")
           .addData("task", task.getId())
           .addData("statusCode", taskStatus.getStatusCode())
           .emit();
      }

      if(taskStatus.isComplete()) {
        if(didPersistStatus) {
          log.info("Task done: %s", task);
          taskLockbox.unlock(task);
          workMayBeAvailable.signalAll();
        } else {
          // TODO: This could be a task-status-submission retry queue instead of retrying the entire task,
          // TODO: which is heavy and probably not necessary.
          log.warn("Status could not be persisted! Reinserting task: %s", task.getId());
          queue.add(task);
        }
      }
    }
    finally {
      giant.unlock();
    }
  }
}
