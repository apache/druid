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
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import com.metamx.common.guava.Comparators;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.emitter.EmittingLogger;
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
import java.util.concurrent.locks.ReentrantLock;

/**
 * Remembers which tasks have locked which intervals. Tasks are permitted to lock an interval if no other task
 * outside their group has locked an overlapping interval for the same datasource. When a task locks an interval,
 * it is assigned a version string that it can use to publish segments.
 */
public class TaskLockbox
{
  // Datasource -> Interval -> Tasks + TaskLock
  private final Map<String, NavigableMap<Interval, TaskLockPosse>> running = Maps.newHashMap();
  private final TaskStorage taskStorage;
  private final ReentrantLock giant = new ReentrantLock();

  private static final EmittingLogger log = new EmittingLogger(TaskLockbox.class);

  public TaskLockbox(TaskStorage taskStorage)
  {
    this.taskStorage = taskStorage;
  }

  /**
   * Attempt to lock a task, without removing it from the queue. Equivalent to the long form of {@code tryLock}
   * with no preferred version.
   *
   * @param task task to attempt to lock
   *
   * @return lock version if lock was acquired, absent otherwise
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
   * @param task             task to attempt to lock
   * @param preferredVersion use this version string if one has not yet been assigned
   *
   * @return lock version if lock was acquired, absent otherwise
   */
  public Optional<TaskLock> tryLock(final Task task, final Interval interval, final Optional<String> preferredVersion)
  {
    giant.lock();

    try {

      if(task.getImplicitLockInterval().isPresent() && !task.getImplicitLockInterval().get().equals(interval)) {
        // Task may only lock its fixed interval, if present
        throw new IAE("Task must lock its fixed interval: %s", task.getId());
      }

      final String dataSource = task.getDataSource();
      final List<TaskLockPosse> foundPosses = findLockPossesForInterval(dataSource, interval);
      final TaskLockPosse posseToUse;

      if (foundPosses.size() > 1) {

        // Too many existing locks.
        return Optional.absent();

      } else if (foundPosses.size() == 1) {

        // One existing lock -- check if we can add to it.

        final TaskLockPosse foundPosse = Iterables.getOnlyElement(foundPosses);
        if (foundPosse.getTaskLock().getInterval().contains(interval) && foundPosse.getTaskLock().getGroupId().equals(task.getGroupId())) {
          posseToUse = foundPosse;
        } else {
          return Optional.absent();
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

      // Add to existing TaskLockPosse, if necessary
      if (posseToUse.getTaskIds().add(task.getId())) {
        log.info("Added task[%s] to TaskLock[%s]", task.getId(), posseToUse.getTaskLock().getGroupId());

        // Best effort to update task storage facility
        try {
          taskStorage.addLock(task.getId(), posseToUse.getTaskLock());
        } catch(Exception e) {
          log.makeAlert("Failed to persist lock in storage")
             .addData("task", task.getId())
             .addData("dataSource", posseToUse.getTaskLock().getDataSource())
             .addData("interval", posseToUse.getTaskLock().getInterval())
             .addData("version", posseToUse.getTaskLock().getVersion())
             .emit();
        }
      } else {
        log.info("Task[%s] already present in TaskLock[%s]", task.getId(), posseToUse.getTaskLock().getGroupId());
      }

      return Optional.of(posseToUse.getTaskLock());
    }
    finally {
      giant.unlock();
    }

  }

  /**
   * Return the currently-active locks for some task.
   *
   * @param task task for which to locate locks
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
    } finally {
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

      // So we can alert if tasks try to release stuff they don't have
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

          // Best effort to remove lock from storage
          try {
            taskStorage.removeLock(task.getId(), taskLock);
          } catch(Exception e) {
            log.makeAlert(e, "Failed to clean up lock from storage")
               .addData("task", task.getId())
               .addData("dataSource", taskLock.getDataSource())
               .addData("interval", taskLock.getInterval())
               .addData("version", taskLock.getVersion())
               .emit();
          }
        }
      }

      if(!removed) {
        log.makeAlert("Lock release without acquire")
           .addData("task", task.getId())
           .addData("interval", interval)
           .emit();
      }
    } finally {
      giant.unlock();
    }
  }

  /**
   * Release all locks for a task. Does nothing if the task is not currently locked.
   *
   * @param task task to unlock
   */
  public void unlock(final Task task)
  {
    giant.lock();

    try {
      for(final TaskLockPosse taskLockPosse : findLockPossesForTask(task)) {
        unlock(task, taskLockPosse.getTaskLock().getInterval());
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Removes all locks from this lockbox.
   */
  public void clear()
  {
    giant.lock();

    try {
      running.clear();
    } finally {
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

      if (task.getImplicitLockInterval().isPresent()) {
        // Narrow down search using findLockPossesForInterval
        searchSpace = findLockPossesForInterval(task.getDataSource(), task.getImplicitLockInterval().get());
      } else {
        // Scan through all locks for this datasource
        final NavigableMap<Interval, TaskLockPosse> dsRunning = running.get(task.getDataSource());
        if(dsRunning == null) {
          searchSpace = ImmutableList.of();
        } else {
          searchSpace = dsRunning.values();
        }
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

  private static class TaskLockPosse
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
    public String toString()
    {
      return Objects.toStringHelper(this)
                    .add("taskLock", taskLock)
                    .add("taskIds", taskIds)
                    .toString();
    }
  }
}
