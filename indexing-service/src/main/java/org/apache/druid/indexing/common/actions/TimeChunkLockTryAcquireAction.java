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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * TaskAction to try to acquire a {@link org.apache.druid.indexing.common.TimeChunkLock}.
 * This action returns null immediately if it fails to get a lock for the given interval.
 */
public class TimeChunkLockTryAcquireAction implements TaskAction<TaskLock>
{
  @JsonIgnore
  private final TaskLockType type;

  @JsonIgnore
  private final Interval interval;

  @JsonCreator
  public TimeChunkLockTryAcquireAction(
      @JsonProperty("lockType") @Nullable TaskLockType type, // nullable for backward compatibility
      @JsonProperty("interval") Interval interval
  )
  {
    this.type = type == null ? TaskLockType.EXCLUSIVE : type;
    this.interval = interval;
  }

  @JsonProperty("lockType")
  public TaskLockType getType()
  {
    return type;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public TypeReference<TaskLock> getReturnTypeReference()
  {
    return new TypeReference<TaskLock>()
    {
    };
  }

  @Override
  public TaskLock perform(Task task, TaskActionToolbox toolbox)
  {
    final LockResult result = toolbox.getTaskLockbox().tryLock(
        task,
        new TimeChunkLockRequest(type, task, interval, null)
    );
    return result.isOk() ? result.getTaskLock() : null;
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "TimeChunkLockTryAcquireAction{" +
           ", type=" + type +
           ", interval=" + interval +
           '}';
  }
}
