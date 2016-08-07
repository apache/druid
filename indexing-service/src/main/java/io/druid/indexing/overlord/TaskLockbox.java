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

import com.google.common.base.Optional;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.actions.TaskLockCriticalState;
import io.druid.indexing.common.task.Task;
import org.joda.time.Interval;

import java.util.List;

/**
 * Remembers which activeTasks have locked which intervals. Tasks are permitted to lock an interval if no other task
 * outside their group has locked an overlapping interval for the same datasource. When a task locks an interval,
 * it is assigned a version string that it can use to publish segments.
 */
public interface TaskLockbox
{
  /**
   * Wipe out our current in-memory state and resync it from our bundled {@link io.druid.indexing.overlord.TaskStorage}.
   */
  void syncFromStorage();
  /**
   * Acquires a lock on behalf of a task. Blocks until the lock is acquired. Throws an exception if the lock
   * cannot be acquired.
   *
   * @param task     task to acquire lock for
   * @param interval interval to lock
   *
   * @return acquired TaskLock
   *
   * @throws java.lang.InterruptedException if the lock cannot be acquired
   */
  TaskLock lock(final Task task, final Interval interval) throws InterruptedException;
  /**
   * Attempt to lock a task, without removing it from the queue. Equivalent to the long form of {@code tryLock}
   * with no preferred version.
   *
   * @param task     task that wants a lock
   * @param interval interval to lock
   *
   * @return lock version if lock was acquired, absent otherwise
   *
   * @throws IllegalStateException if the task is not a valid active task
   */
  Optional<TaskLock> tryLock(final Task task, final Interval interval);
  /**
   * Return the currently-active locks for some task.
   *
   * @param task task for which to locate locks
   *
   * @return currently-active locks for the given task
   */
  List<TaskLock> findLocksForTask(final Task task);
  /**
   * Release lock held for a task on a particular interval. Does nothing if the task does not currently
   * hold the mentioned lock.
   *
   * @param task     task to unlock
   * @param interval interval to unlock
   */
  void unlock(final Task task, final Interval interval);
  /**
   * Release all locks for a task and remove task from set of active tasks. Does nothing if the task is not currently locked or not an active task.
   *
   * @param task task to unlock
   */
  void remove(final Task task);
  void add(Task task);
  /**
   * Sets the TaskLock state specified by <code>taskLockCriticalState</code> for <code>task</> with <code>interval</code>
   * Only applicable when performing priority based task locking
   *
   * @param task                  task corresponding to the lock
   * @param interval              interval for the lock
   * @param taskLockCriticalState upgrade or downgrade the lock depending on this parameter
   *
   * @return true if the TaskLock was set, false otherwise
   */
  boolean setTaskLockCriticalState(Task task, Interval interval, TaskLockCriticalState taskLockCriticalState);
}
