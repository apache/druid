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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * This class represents the result of {@link TaskLockbox#tryLock(TaskLockType, Task, Interval)}. If the lock
 * acquisition fails, the callers can tell that it was failed because it was preempted by other locks of higher
 * priorities or not by checking the {@link #revoked} flag.
 *
 * The {@link #revoked} flag means that consecutive lock acquisitions for the same dataSource and interval are
 * returning different locks because another lock of a higher priority preempted your lock at some point. In this case,
 * the lock acquisition must fail.
 *
 * @see TaskLockbox#tryLock(TaskLockType, Task, Interval)
 */
public class LockResult
{
  private final TaskLock taskLock;
  private final boolean revoked;

  public static LockResult ok(TaskLock taskLock)
  {
    return new LockResult(taskLock, false);
  }

  public static LockResult fail(boolean revoked)
  {
    return new LockResult(null, revoked);
  }

  @JsonCreator
  public LockResult(
      @JsonProperty("taskLock") @Nullable TaskLock taskLock,
      @JsonProperty("revoked") boolean revoked
  )
  {
    this.taskLock = taskLock;
    this.revoked = revoked;
  }

  @JsonProperty("taskLock")
  public TaskLock getTaskLock()
  {
    return taskLock;
  }

  @JsonProperty("revoked")
  public boolean isRevoked()
  {
    return revoked;
  }

  public boolean isOk()
  {
    return taskLock != null;
  }
}
