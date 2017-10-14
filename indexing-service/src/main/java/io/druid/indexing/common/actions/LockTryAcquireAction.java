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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.LockResult;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class LockTryAcquireAction implements TaskAction<TaskLock>
{
  @JsonIgnore
  private final TaskLockType type;

  @JsonIgnore
  private final Interval interval;

  @JsonCreator
  public LockTryAcquireAction(
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
    final LockResult result = toolbox.getTaskLockbox().tryLock(type, task, interval);
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
    return "LockTryAcquireAction{" +
           "lockType=" + type +
           ", interval=" + interval +
           '}';
  }
}
