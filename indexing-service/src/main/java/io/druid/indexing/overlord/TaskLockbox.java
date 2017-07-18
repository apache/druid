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

package io.druid.indexing.overlord;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.common.utils.JodaUtils;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.server.initialization.ServerConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Remembers which activeTasks have locked which intervals. Tasks are permitted to lock an interval if no other task
 * outside their group has locked an overlapping interval for the same datasource. When a task locks an interval,
 * it is assigned a version string that it can use to publish segments.
 */
public class TaskLockbox
{
  // Datasource -> Interval -> Tasks + TaskLock
  private final Map<String, NavigableMap<Interval, TaskLockPosse>> running = Maps.newHashMap();
  private final TaskStorage taskStorage;
  private final ReentrantLock giant = new ReentrantLock(true);
  private final Condition lockReleaseCondition = giant.newCondition();
  protected final long lockTimeoutMillis;

  private static final EmittingLogger log = new EmittingLogger(TaskLockbox.class);

  // Stores List of Active Tasks. TaskLockbox will only grant locks to active activeTasks.
  // this set should be accessed under the giant lock.
  private final Set<String> activeTasks = Sets.newHashSet();

  @Inject
  public TaskLockbox(
      TaskStorage taskStorage,
      ServerConfig serverConfig
  )
  {
    this.taskStorage = taskStorage;
    this.lockTimeoutMillis = serverConfig.getMaxIdleTime().getMillis();
  }

  public TaskLockbox(
      TaskStorage taskStorage,
      long lockTimeoutMillis
  )
  {
    this.taskStorage = taskStorage;
    this.lockTimeoutMillis = lockTimeoutMillis;
  }

  /**
   * Wipe out our current in-memory state and resync it from our bundled {@link TaskStorage}.
   */
  public void syncFromStorage()
  {
    giant.lock();

    try {
      // Load stuff from taskStorage first. If this fails, we don't want to lose all our locks.
      final Set<String> storedActiveTasks = Sets.newHashSet();
      final List<Pair<Task, TaskLock>> storedLocks = Lists.newArrayList();
      for (final Task task : taskStorage.getActiveTasks()) {
        storedActiveTasks.add(task.getId());
        for (final TaskLock taskLock : taskStorage.getLocks(task.getId())) {
          storedLocks.add(Pair.of(task, taskLock));
        }
      }
      // Sort locks by version, so we add them back in the order they were acquired.
      final Ordering<Pair<Task, TaskLock>> byVersionOrdering = new Ordering<Pair<Task, TaskLock>>()
      {
        @Override
        public int compare(Pair<Task, TaskLock> left, Pair<Task, TaskLock> right)
        {
          // The second compare shouldn't be necessary, but, whatever.
          return ComparisonChain.start()
                                .compare(left.rhs.getVersion(), right.rhs.getVersion())
                                .compare(left.lhs.getId(), right.lhs.getId())
                                .result();
        }
      };
      running.clear();
      activeTasks.clear();
      activeTasks.addAll(storedActiveTasks);
      // Bookkeeping for a log message at the end
      int taskLockCount = 0;
      for (final Pair<Task, TaskLock> taskAndLock : byVersionOrdering.sortedCopy(storedLocks)) {
        final Task task = taskAndLock.lhs;
        final TaskLock savedTaskLock = taskAndLock.rhs;
        if (savedTaskLock.getInterval().toDurationMillis() <= 0) {
          // "Impossible", but you never know what crazy stuff can be restored from storage.
          log.warn("WTF?! Got lock with empty interval for task: %s", task.getId());
          continue;
        }

        final TaskLockPosse taskLockPosse = tryAddTaskToLockPosse(
            task,
            savedTaskLock.getInterval(),
            Optional.of(savedTaskLock.getVersion())
        );
        if (taskLockPosse != null) {
          taskLockPosse.getTaskIds().add(task.getId());

          final TaskLock taskLock = taskLockPosse.getTaskLock();

          if (savedTaskLock.getVersion().equals(taskLock.getVersion())) {
            taskLockCount ++;
            log.info(
                "Reacquired lock on interval[%s] version[%s] for task: %s",
                savedTaskLock.getInterval(),
                savedTaskLock.getVersion(),
                task.getId()
            );
          } else {
            taskLockCount ++;
            log.info(
                "Could not reacquire lock on interval[%s] version[%s] (got version[%s] instead) for task: %s",
                savedTaskLock.getInterval(),
                savedTaskLock.getVersion(),
                taskLock.getVersion(),
                task.getId()
            );
          }
        } else {
          throw new ISE(
              "Could not reacquire lock on interval[%s] version[%s] for task: %s",
              savedTaskLock.getInterval(),
              savedTaskLock.getVersion(),
              task.getId()
          );
        }
      }
      log.info(
          "Synced %,d locks for %,d activeTasks from storage (%,d locks ignored).",
          taskLockCount,
          activeTasks.size(),
          storedLocks.size() - taskLockCount
      );
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Acquires a lock on behalf of a task. Blocks until the lock is acquired. Throws an exception if the lock
   * cannot be acquired.
   *
   * @param task task to acquire lock for
   * @param interval interval to lock
   * @return acquired TaskLock
   *
   * @throws InterruptedException if the lock cannot be acquired
   */
  public TaskLock lock(final Task task, final Interval interval) throws InterruptedException
  {
    long timeout = lockTimeoutMillis;
    giant.lock();
    try {
      Optional<TaskLock> taskLock;
      while (!(taskLock = tryLock(task, interval)).isPresent()) {
        long startTime = System.currentTimeMillis();
        lockReleaseCondition.await(timeout, TimeUnit.MILLISECONDS);
        long timeDelta = System.currentTimeMillis() - startTime;
        if (timeDelta >= timeout) {
          log.error(
              "Task [%s] can not acquire lock for interval [%s] within [%s] ms",
              task.getId(),
              interval,
              lockTimeoutMillis
          );

          throw new InterruptedException(String.format(
              "Task [%s] can not acquire lock for interval [%s] within [%s] ms",
              task.getId(),
              interval,
              lockTimeoutMillis
          ));
        } else {
          timeout -= timeDelta;
        }
      }

      return taskLock.get();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Attempt to lock a task, without removing it from the queue. Equivalent to the long form of {@code tryLock}
   * with no preferred version.
   *
   * @param task             task that wants a lock
   * @param interval         interval to lock
   *
   * @return lock version if lock was acquired, absent otherwise
   * @throws IllegalStateException if the task is not a valid active task
   */
  public Optional<TaskLock> tryLock(final Task task, final Interval interval)
  {
    return tryLock(task, interval, Optional.<String>absent());
  }

  /**
   * Attempt to lock a task, without removing it from the queue. Can safely be called multiple times on the same task.
   * This method will attempt to assign version strings that obey the invariant that every version string is
   * lexicographically greater than any other version string previously assigned to the same interval. This invariant
   * is only mostly guaranteed, however; we assume clock monotonicity and we assume that callers specifying
   * {@code preferredVersion} are doing the right thing.
   *
   * @param task             task that wants a lock
   * @param interval         interval to lock
   * @param preferredVersion use this version string if one has not yet been assigned
   *
   * @return lock version if lock was acquired, absent otherwise
   * @throws IllegalStateException if the task is not a valid active task
   */
  private Optional<TaskLock> tryLock(final Task task, final Interval interval, final Optional<String> preferredVersion)
  {
    giant.lock();

    try {
      if(!activeTasks.contains(task.getId())){
        throw new ISE("Unable to grant lock to inactive Task [%s]", task.getId());
      }
      Preconditions.checkArgument(interval.toDurationMillis() > 0, "interval empty");

      final TaskLockPosse posseToUse = tryAddTaskToLockPosse(task, interval, preferredVersion);
      if (posseToUse != null) {
        // Add to existing TaskLockPosse, if necessary
        if (posseToUse.getTaskIds().add(task.getId())) {
          log.info("Added task[%s] to TaskLock[%s]", task.getId(), posseToUse.getTaskLock().getGroupId());

          // Update task storage facility. If it fails, revoke the lock.
          try {
            taskStorage.addLock(task.getId(), posseToUse.getTaskLock());
            return Optional.of(posseToUse.getTaskLock());
          }
          catch (Exception e) {
            log.makeAlert("Failed to persist lock in storage")
               .addData("task", task.getId())
               .addData("dataSource", posseToUse.getTaskLock().getDataSource())
               .addData("interval", posseToUse.getTaskLock().getInterval())
               .addData("version", posseToUse.getTaskLock().getVersion())
               .emit();
            unlock(task, interval);
            return Optional.absent();
          }
        } else {
          log.info("Task[%s] already present in TaskLock[%s]", task.getId(), posseToUse.getTaskLock().getGroupId());
          return Optional.of(posseToUse.getTaskLock());
        }

      } else {
        return Optional.absent();
      }
    }
    finally {
      giant.unlock();
    }

  }

  private TaskLockPosse tryAddTaskToLockPosse(
      final Task task,
      final Interval interval,
      final Optional<String> preferredVersion
  )
  {
    giant.lock();

    try {
      final String dataSource = task.getDataSource();
      final List<TaskLockPosse> foundPosses = findLockPossesForInterval(dataSource, interval);
      final TaskLockPosse posseToUse;

      if (foundPosses.size() > 1) {

        // Too many existing locks.
        return null;

      } else if (foundPosses.size() == 1) {

        // One existing lock -- check if we can add to it.

        final TaskLockPosse foundPosse = Iterables.getOnlyElement(foundPosses);
        if (foundPosse.getTaskLock().getInterval().contains(interval) && foundPosse.getTaskLock().getGroupId().equals(task.getGroupId())) {
          posseToUse = foundPosse;
        } else {
          //Could be a deadlock for LockAcquireAction: same task trying to acquire lock for overlapping interval
          if (foundPosse.getTaskIds().contains(task.getId())) {
            log.makeAlert("Same Task is trying to acquire lock for overlapping interval")
               .addData("task", task.getId())
               .addData("interval", interval);
          }
          return null;
        }

      } else {

        // No existing locks. We can make a new one.
        if (!running.containsKey(dataSource)) {
          running.put(dataSource, new TreeMap<Interval, TaskLockPosse>(Comparators.intervalsByStartThenEnd()));
        }

        // Create new TaskLock and assign it a version.
        // Assumption: We'll choose a version that is greater than any previously-chosen version for our interval. (This
        // may not always be true, unfortunately. See below.)

        final String version;

        if (preferredVersion.isPresent()) {
          // We have a preferred version. We'll trust our caller to not break our ordering assumptions and just use it.
          version = preferredVersion.get();
        } else {
          // We are running under an interval lock right now, so just using the current time works as long as we can trust
          // our clock to be monotonic and have enough resolution since the last time we created a TaskLock for the same
          // interval. This may not always be true; to assure it we would need to use some method of timekeeping other
          // than the wall clock.
          version = new DateTime().toString();
        }

        posseToUse = new TaskLockPosse(new TaskLock(task.getGroupId(), dataSource, interval, version));
        running.get(dataSource)
               .put(interval, posseToUse);

        log.info("Created new TaskLockPosse: %s", posseToUse);
      }

      return posseToUse;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return the currently-active locks for some task.
   *
   * @param task task for which to locate locks
   * @return currently-active locks for the given task
   */
  public List<TaskLock> findLocksForTask(final Task task)
  {
    giant.lock();

    try {
      return Lists.transform(
          findLockPossesForTask(task), new Function<TaskLockPosse, TaskLock>()
          {
            @Override
            public TaskLock apply(TaskLockPosse taskLockPosse)
            {
              return taskLockPosse.getTaskLock();
            }
          }
      );
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Release lock held for a task on a particular interval. Does nothing if the task does not currently
   * hold the mentioned lock.
   *
   * @param task task to unlock
   * @param interval interval to unlock
   */
  public void unlock(final Task task, final Interval interval)
  {
    giant.lock();

    try {
      final String dataSource = task.getDataSource();
      final NavigableMap<Interval, TaskLockPosse> dsRunning = running.get(dataSource);

      // So we can alert if activeTasks try to release stuff they don't have
      boolean removed = false;

      if(dsRunning != null) {
        final TaskLockPosse taskLockPosse = dsRunning.get(interval);
        if(taskLockPosse != null) {
          final TaskLock taskLock = taskLockPosse.getTaskLock();

          // Remove task from live list
          log.info("Removing task[%s] from TaskLock[%s]", task.getId(), taskLock.getGroupId());
          removed = taskLockPosse.getTaskIds().remove(task.getId());

          if (taskLockPosse.getTaskIds().isEmpty()) {
            log.info("TaskLock is now empty: %s", taskLock);
            running.get(dataSource).remove(taskLock.getInterval());
          }

          if (running.get(dataSource).size() == 0) {
            running.remove(dataSource);
          }

          // Wake up blocking-lock waiters
          lockReleaseCondition.signalAll();

          // Remove lock from storage. If it cannot be removed, just ignore the failure.
          try {
            taskStorage.removeLock(task.getId(), taskLock);
          }
          catch (Exception e) {
            log.makeAlert(e, "Failed to clean up lock from storage")
               .addData("task", task.getId())
               .addData("dataSource", taskLock.getDataSource())
               .addData("interval", taskLock.getInterval())
               .addData("version", taskLock.getVersion())
               .emit();
          }
        }
      }

      if (!removed) {
        log.makeAlert("Lock release without acquire")
           .addData("task", task.getId())
           .addData("interval", interval)
           .emit();
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Release all locks for a task and remove task from set of active tasks. Does nothing if the task is not currently locked or not an active task.
   *
   * @param task task to unlock
   */
  public void remove(final Task task)
  {
    giant.lock();
    try {
      try {
        log.info("Removing task[%s] from activeTasks", task.getId());
        for (final TaskLockPosse taskLockPosse : findLockPossesForTask(task)) {
          unlock(task, taskLockPosse.getTaskLock().getInterval());
        }
      }
      finally {
        activeTasks.remove(task.getId());
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return the currently-active lock posses for some task.
   *
   * @param task task for which to locate locks
   */
  private List<TaskLockPosse> findLockPossesForTask(final Task task)
  {
    giant.lock();

    try {
      final Iterable<TaskLockPosse> searchSpace;

      // Scan through all locks for this datasource
      final NavigableMap<Interval, TaskLockPosse> dsRunning = running.get(task.getDataSource());
      if(dsRunning == null) {
        searchSpace = ImmutableList.of();
      } else {
        searchSpace = dsRunning.values();
      }

      return ImmutableList.copyOf(
          Iterables.filter(
              searchSpace, new Predicate<TaskLockPosse>()
          {
            @Override
            public boolean apply(TaskLockPosse taskLock)
            {
              return taskLock.getTaskIds().contains(task.getId());
            }
          }
          )
      );
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return all locks that overlap some search interval.
   */
  private List<TaskLockPosse> findLockPossesForInterval(final String dataSource, final Interval interval)
  {
    giant.lock();

    try {
      final NavigableMap<Interval, TaskLockPosse> dsRunning = running.get(dataSource);
      if (dsRunning == null) {
        // No locks at all
        return Collections.emptyList();
      } else {
        // Tasks are indexed by locked interval, which are sorted by interval start. Intervals are non-overlapping, so:
        final NavigableSet<Interval> dsLockbox = dsRunning.navigableKeySet();
        final Iterable<Interval> searchIntervals = Iterables.concat(
            // Single interval that starts at or before ours
            Collections.singletonList(dsLockbox.floor(new Interval(interval.getStart(), new DateTime(JodaUtils.MAX_INSTANT)))),

            // All intervals that start somewhere between our start instant (exclusive) and end instant (exclusive)
            dsLockbox.subSet(
                new Interval(interval.getStart(), new DateTime(JodaUtils.MAX_INSTANT)),
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
                    new Function<Interval, TaskLockPosse>()
                    {
                      @Override
                      public TaskLockPosse apply(Interval interval)
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

  public void add(Task task)
  {
    giant.lock();
    try {
      log.info("Adding task[%s] to activeTasks", task.getId());
      activeTasks.add(task.getId());
    }
    finally {
      giant.unlock();
    }
  }

  @VisibleForTesting
  Set<String> getActiveTasks()
  {
    return activeTasks;
  }

  @VisibleForTesting
  Map<String, NavigableMap<Interval, TaskLockPosse>> getAllLocks()
  {
    return running;
  }

  static class TaskLockPosse
  {
    final private TaskLock taskLock;
    final private Set<String> taskIds;

    public TaskLockPosse(TaskLock taskLock)
    {
      this.taskLock = taskLock;
      taskIds = Sets.newHashSet();
    }

    public TaskLock getTaskLock()
    {
      return taskLock;
    }

    public Set<String> getTaskIds()
    {
      return taskIds;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }

      if (!getClass().equals(o.getClass())) {
        return false;
      }

      final TaskLockPosse that = (TaskLockPosse) o;
      if (!taskLock.equals(that.taskLock)) {
        return false;
      }

      return taskIds.equals(that.taskIds);
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(taskLock, taskIds);
    }

    @Override
    public String toString()
    {
      return Objects.toStringHelper(this)
                    .add("taskLock", taskLock)
                    .add("taskIds", taskIds)
                    .toString();
    }
  }
}
