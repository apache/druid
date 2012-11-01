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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Booleans;
import com.metamx.common.Pair;
import com.metamx.common.guava.Comparators;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Interface between task producers and task consumers.
 *
 * The queue accepts tasks from producers using {@link #add} and delivers tasks to consumers using either
 * {@link #take} or {@link #poll}. Ordering is mostly-FIFO, with deviations when the natural next task would conflict
 * with a currently-running task. In that case, tasks are skipped until a runnable one is found.
 *
 * To manage locking, the queue keeps track of currently-running tasks as {@link TaskGroup} objects. The idea is that
 * only one TaskGroup can be running on a particular dataSource + interval, and that TaskGroup has a single version
 * string that all tasks in the group must use to publish segments. Tasks in the same TaskGroup may run concurrently.
 *
 * For persistence, the queue saves new tasks from {@link #add} and task status updates from {@link #done} using a
 * {@link TaskStorage} object.
 *
 * To support leader election of our containing system, the queue can be stopped (in which case it will not accept
 * any new tasks, or hand out any more tasks, until started again).
 */
public class TaskQueue
{
  private final List<Task> queue = Lists.newLinkedList();
  private final Map<String, NavigableMap<Interval, TaskGroup>> running = Maps.newHashMap();
  private final TaskStorage taskStorage;

  private final ReentrantLock giant = new ReentrantLock();
  private final Condition workMayBeAvailable = giant.newCondition();

  private volatile boolean active = false;

  private static final Logger log = new Logger(TaskQueue.class);

  public TaskQueue(TaskStorage taskStorage)
  {
    this.taskStorage = Preconditions.checkNotNull(taskStorage, "taskStorage");
  }

  /**
   * Starts this task queue. Loads tasks from our task storage facility and allows {@link #add(Task)} to accept
   * new tasks. This should not be called on an already-started queue.
   */
  @LifecycleStart
  public void start()
  {
    giant.lock();

    try {

      Preconditions.checkState(!active, "queue was already started!");
      Preconditions.checkState(queue.isEmpty(), "queue must be empty!");
      Preconditions.checkState(running.isEmpty(), "running list must be empty!");

      // XXX - We might want a TaskStorage API that does this, but including the Pair type in the interface seems clumsy.
      final List<Pair<Task, String>> runningTasks = Lists.transform(
          taskStorage.getRunningTasks(),
          new Function<Task, Pair<Task, String>>()
          {
            @Override
            public Pair<Task, String> apply(Task task)
            {
              return Pair.of(task, taskStorage.getVersion(task.getId()).orNull());
            }
          }
      );

      // Sort by version, with nulls last
      final Ordering<Pair<Task, String>> byVersionOrdering = new Ordering<Pair<Task, String>>()
      {
        final private Ordering<String> baseOrdering = Ordering.natural().nullsLast();

        @Override
        public int compare(
            Pair<Task, String> left, Pair<Task, String> right
        )
        {
          return baseOrdering.compare(left.rhs, right.rhs);
        }
      };

      for(final Pair<Task, String> taskAndVersion : byVersionOrdering.sortedCopy(runningTasks)) {
        final Task task = taskAndVersion.lhs;
        final String preferredVersion = taskAndVersion.rhs;

        queue.add(task);

        if(preferredVersion != null) {
          final Optional<String> version = tryLock(task, Optional.of(preferredVersion));

          log.info(
              "Bootstrapped task[%s] with preferred version[%s]: %s",
              task.getId(),
              preferredVersion,
              version.isPresent() ? String.format("locked with version[%s]", version.get()) : "not lockable"
          );
        } else {
          log.info("Bootstrapped task[%s] with no preferred version", task.getId());
        }
      }

      log.info("Bootstrapped %,d tasks. Ready to go!", runningTasks.size());

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

      queue.clear();
      running.clear();
      active = false;

    } finally {
      giant.unlock();
    }
  }

  /**
   * Adds some work to the queue and the underlying task storage facility with a generic "running" status.
   *
   * @param task task to add
   * @return true
   */
  public boolean add(Task task)
  {
    giant.lock();

    try {
      Preconditions.checkState(active, "Queue is not active!");

      taskStorage.insert(task, TaskStatus.running(task.getId()));

      queue.add(task);
      workMayBeAvailable.signalAll();

      return true;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Locks and returns next doable work from the queue. Blocks if there is no doable work.
   * @return runnable task
   */
  public VersionedTaskWrapper take() throws InterruptedException
  {
    giant.lock();

    try {
      VersionedTaskWrapper taskWrapper;

      while((taskWrapper = poll()) == null) {
        log.info("Waiting for work...");
        workMayBeAvailable.await();
      }

      return taskWrapper;
    } finally {
      giant.unlock();
    }
  }

  /**
   * Locks and removes next doable work from the queue. Returns null if there is no doable work.
   * @return runnable task or null
   */
  public VersionedTaskWrapper poll()
  {
    giant.lock();

    try {
      log.info("Checking for doable work");
      for(final Task task : queue) {
        final Optional<String> maybeVersion = tryLock(task);
        if(maybeVersion.isPresent()) {
          Preconditions.checkState(active, "wtf? Found task when inactive");
          taskStorage.setVersion(task.getId(), maybeVersion.get());
          queue.remove(task);
          log.info("Task claimed: %s", task);
          return new VersionedTaskWrapper(task, maybeVersion.get());
        }
      }

      log.info("No doable work found.");
      return null;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Unlock some work. Does not update the task storage facility. Throws an exception if this work is not currently
   * running.
   *
   * @param task task to unlock
   * @throws IllegalStateException if task is not currently locked
   */
  private void unlock(final Task task)
  {
    giant.lock();

    try {
      final String dataSource = task.getDataSource();
      final Interval interval = task.getInterval();

      final List<TaskGroup> maybeTaskGroup = Lists.newArrayList(
          FunctionalIterable.create(findLocks(dataSource, interval))
                            .filter(
                                new Predicate<TaskGroup>()
                                {
                                  @Override
                                  public boolean apply(TaskGroup taskGroup)
                                  {
                                    return taskGroup.getTaskSet().contains(task);
                                  }
                                }
                            )
      );

      final TaskGroup taskGroup;
      if(maybeTaskGroup.size() == 1) {
        taskGroup = maybeTaskGroup.get(0);
      } else {
        throw new IllegalStateException(String.format("Task must be running: %s", task.getId()));
      }

      // Remove task from live list
      log.info("Removing task[%s] from TaskGroup[%s]", task.getId(), taskGroup.getGroupId());
      taskGroup.getTaskSet().remove(task);

      if(taskGroup.getTaskSet().size() == 0) {
        log.info("TaskGroup complete: %s", taskGroup);
        running.get(dataSource).remove(taskGroup.getInterval());
      }

      if(running.get(dataSource).size() == 0) {
        running.remove(dataSource);
      }

      workMayBeAvailable.signalAll();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Unlock some task and update its status in the task storage facility. If "status" is a continuation status (i.e.
   * it has nextTasks) this will add the next tasks to the queue with a generic running status.
   *
   * @param task task to unlock
   * @param status task completion status; must not be runnable
   * @throws IllegalStateException if task is not currently running, or if status is runnable
   */
  public void done(final Task task, final TaskStatus status)
  {
    giant.lock();

    try {
      Preconditions.checkState(active, "Queue is not active!");
      Preconditions.checkState(!status.isRunnable(), "status must no longer be runnable");

      Preconditions.checkState(
          task.getId().equals(status.getId()),
          "Mismatching task ids[%s/%s]",
          task.getId(),
          status.getId()
      );

      // Might change on continuation failure
      TaskStatus actualStatus = status;

      // Add next tasks, if any
      try {
        for(final Task nextTask : status.getNextTasks()) {
          add(nextTask);
          tryLock(nextTask);
        }
      } catch(Exception e) {
        log.error(e, "Failed to continue task: %s", task.getId());
        actualStatus = TaskStatus.failure(task.getId());
      }

      unlock(task);

      // Update status in DB
      taskStorage.setStatus(task.getId(), actualStatus);

      log.info("Task done: %s", task);
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Returns task status for a particular task ID. May collapse "continued" statuses down to "success" or "failure"
   * if appropriate.
   */
  public Optional<TaskStatus> getStatus(final String taskid)
  {
    giant.lock();

    try {
      final Optional<TaskStatus> statusOptional = taskStorage.getStatus(taskid);
      if(statusOptional.isPresent()) {
        // See if we can collapse this down
        return Optional.of(collapseStatus(statusOptional.get()));
      } else {
        return statusOptional;
      }
    }
    finally {
      giant.unlock();
    }
  }

  private TaskStatus collapseStatus(TaskStatus status)
  {

    if (status.isContinued()) {

      int nSubtasks = 0;
      int nSuccesses = 0;
      List<DataSegment> segments = Lists.newArrayList();

      for(final Task subtask : status.getNextTasks()) {

        final TaskStatus subtaskStatus = collapseStatus(taskStorage.getStatus(subtask.getId()).get());
        nSubtasks ++;

        if (subtaskStatus.isFailure()) {
          return TaskStatus.failure(status.getId());
        } else if (subtaskStatus.isSuccess()) {
          nSuccesses++;
          segments.addAll(subtaskStatus.getSegments());
        }

      }

      if (nSubtasks == nSuccesses) {
        return TaskStatus.success(status.getId(), segments);
      }

    }

    // unable to collapse it down
    return status;

  }

  /**
   * Attempt to lock a task, without removing it from the queue. Can safely be called multiple times on the same task.
   *
   * @param task task to attempt to lock
   * @return lock version if lock was acquired, absent otherwise
   */
  private Optional<String> tryLock(final Task task)
  {
    return tryLock(task, Optional.<String>absent());
  }

  /**
   * Attempt to lock a task, without removing it from the queue. Can safely be called multiple times on the same task.
   *
   * @param task task to attempt to lock
   * @param preferredVersion use this version if possible (no guarantees, though!)
   * @return lock version if lock was acquired, absent otherwise
   */
  private Optional<String> tryLock(final Task task, final Optional<String> preferredVersion)
  {
    giant.lock();

    try {

      final String dataSource = task.getDataSource();
      final Interval interval = task.getInterval();

      final List<TaskGroup> foundLocks = findLocks(dataSource, interval);
      final TaskGroup taskGroupToUse;

      if (foundLocks.size() > 1) {

        // Too many existing locks.
        return Optional.absent();

      } else if (foundLocks.size() == 1) {

        // One existing lock -- check if we can add to it.

        final TaskGroup foundLock = Iterables.getOnlyElement(foundLocks);
        if (foundLock.getInterval().contains(interval) && foundLock.getGroupId().equals(task.getGroupId())) {
          taskGroupToUse = foundLock;
        } else {
          return Optional.absent();
        }

      } else {

        // No existing locks. We can make a new one.
        if (!running.containsKey(dataSource)) {
          running.put(dataSource, new TreeMap<Interval, TaskGroup>(Comparators.intervalsByStartThenEnd()));
        }

        // Create new TaskGroup and assign it a version.
        // Assumption: We'll choose a version that is greater than any previously-chosen version for our interval. (This
        // may not always be true, unfortunately. See below.)

        final String version;

        if(preferredVersion.isPresent()) {
          // We have a preferred version. Since this is a private method, we'll trust our caller to not break our
          // ordering assumptions and just use it.
          version = preferredVersion.get();
        } else {
          // We are running under an interval lock right now, so just using the current time works as long as we can trust
          // our clock to be monotonic and have enough resolution since the last time we created a TaskGroup for the same
          // interval. This may not always be true; to assure it we would need to use some method of timekeeping other
          // than the wall clock.
          version = new DateTime().toString();
        }

        taskGroupToUse = new TaskGroup(task.getGroupId(), dataSource, interval, version);
        running.get(dataSource)
               .put(interval, taskGroupToUse);

        log.info("Created new TaskGroup[%s]", taskGroupToUse);

      }

      // Add to existing TaskGroup, if necessary
      if (taskGroupToUse.getTaskSet().add(task)) {
        log.info("Added task[%s] to TaskGroup[%s]", task.getId(), taskGroupToUse.getGroupId());
      } else {
        log.info("Task[%s] already present in TaskGroup[%s]", task.getId(), taskGroupToUse.getGroupId());
      }

      return Optional.of(taskGroupToUse.getVersion());

    } finally {
      giant.unlock();
    }

  }

  /**
   * Return all locks that overlap some search interval.
   */
  private List<TaskGroup> findLocks(final String dataSource, final Interval interval)
  {
    giant.lock();

    try {
      final NavigableMap<Interval, TaskGroup> dsRunning = running.get(dataSource);
      if(dsRunning == null) {
        // No locks at all
        return Collections.emptyList();
      } else {
        // Tasks are indexed by locked interval, which are sorted by interval start. Intervals are non-overlapping, so:
        final NavigableSet<Interval> dsLockbox = dsRunning.navigableKeySet();
        final Iterable<Interval> searchIntervals = Iterables.concat(
            // Single interval that starts at or before ours
            Collections.singletonList(dsLockbox.floor(new Interval(interval.getStart(), new DateTime(Long.MAX_VALUE)))),

            // All intervals that start somewhere between our start instant (exclusive) and end instant (exclusive)
            dsLockbox.subSet(
                new Interval(interval.getStart(), new DateTime(Long.MAX_VALUE)),
                false,
                new Interval(interval.getEnd(), interval.getEnd()),
                false
            )
        );

        return Lists.newArrayList(
            FunctionalIterable
                .create(searchIntervals)
                .filter(
                    new Predicate<Interval>()
                    {
                      @Override
                      public boolean apply(@Nullable Interval searchInterval)
                      {
                        return searchInterval != null && searchInterval.overlaps(interval);
                      }
                    }
                )
                .transform(
                    new Function<Interval, TaskGroup>()
                    {
                      @Override
                      public TaskGroup apply(Interval interval)
                      {
                        return dsRunning.get(interval);
                      }
                    }
                )
        );
      }
    }
    finally {
      giant.unlock();
    }
  }
}
