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

import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.task.Task;
import org.joda.time.Interval;

/**
 * This class represents an action must be done while the task's lock is guaranteed to not be revoked in the middle of
 * action.
 *
 * Implementations must not change the lock state by calling {@link TaskLockbox#lock(TaskLockType, Task, Interval)},
 * {@link TaskLockbox#lock(TaskLockType, Task, Interval, long)}, {@link TaskLockbox#tryLock(TaskLockType, Task, Interval)},
 * or {@link TaskLockbox#unlock(Task, Interval)}.
 *
 * Also, implementations should be finished as soon as possible because all methods in {@link TaskLockbox} are blocked
 * until this action is finished.
 *
 * @see TaskLockbox#doInCriticalSection(Task, List, CriticalAction, CriticalAction)
 */
public interface CriticalAction<T>
{
  T perform() throws Exception;
}
