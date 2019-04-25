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

import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.task.Task;
import org.joda.time.Interval;

/**
 * This class represents a critical action must be done while the task's lock is guaranteed to not be revoked in the
 * middle of the action.
 *
 * Implementations must not change the lock state by calling {@link TaskLockbox#lock)}, {@link TaskLockbox#tryLock)},
 * or {@link TaskLockbox#unlock(Task, Interval)}.
 *
 * Also, implementations should be finished as soon as possible because all methods in {@link TaskLockbox} are blocked
 * until this action is finished.
 *
 * @see TaskLockbox#doInCriticalSection
 */
public class CriticalAction<T>
{
  private final Action<T> actionOnValidLocks;
  private final Action<T> actionOnInvalidLocks;

  private CriticalAction(Action<T> actionOnValidLocks, Action<T> actionOnInvalidLocks)
  {
    this.actionOnValidLocks = Preconditions.checkNotNull(actionOnValidLocks, "actionOnValidLocks");
    this.actionOnInvalidLocks = Preconditions.checkNotNull(actionOnInvalidLocks, "actionOnInvalidLocks");
  }

  T perform(boolean isTaskLocksValid) throws Exception
  {
    return isTaskLocksValid ? actionOnValidLocks.perform() : actionOnInvalidLocks.perform();
  }

  public static <T> Builder<T> builder()
  {
    return new Builder<>();
  }

  public static class Builder<T>
  {
    private Action<T> actionOnInvalidLocks;
    private Action<T> actionOnValidLocks;

    public Builder<T> onValidLocks(Action<T> action)
    {
      this.actionOnValidLocks = action;
      return this;
    }

    public Builder<T> onInvalidLocks(Action<T> action)
    {
      this.actionOnInvalidLocks = action;
      return this;
    }

    public CriticalAction<T> build()
    {
      return new CriticalAction<>(actionOnValidLocks, actionOnInvalidLocks);
    }
  }

  public interface Action<T>
  {
    T perform() throws Exception;
  }
}
