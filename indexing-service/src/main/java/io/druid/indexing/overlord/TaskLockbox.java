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

public interface TaskLockbox
{
  void syncFromStorage();
  TaskLock lock(final Task task, final Interval interval) throws InterruptedException;
  Optional<TaskLock> tryLock(final Task task, final Interval interval);
  List<TaskLock> findLocksForTask(final Task task);
  void unlock(final Task task, final Interval interval);
  void remove(final Task task);
  void add(Task task);
  boolean setTaskLockCriticalState(Task task, Interval interval, TaskLockCriticalState taskLockCriticalState);
}
