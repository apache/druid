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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.LockResult;
import io.druid.java.util.common.ISE;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class SurrogateLockTryAcquireAction implements TaskAction<TaskLock>
{
  private final TaskLockType type;

  private final Interval interval;

  private final String surrogateId;

  @JsonCreator
  public SurrogateLockTryAcquireAction(
      @JsonProperty("lockType") @Nullable TaskLockType type,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("surrogateId") String surrogateId
  )
  {
    this.type = type;
    this.interval = interval;
    this.surrogateId = surrogateId;
  }

  @JsonProperty("lockType")
  public TaskLockType getType()
  {
    return type;
  }

  @JsonProperty("interval")
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty("surrogateId")
  public String getSurrogateId()
  {
    return surrogateId;
  }

  @Override
  public TypeReference<TaskLock> getReturnTypeReference()
  {
    return new TypeReference<TaskLock>()
    {
    };
  }

  @Override
  public TaskLock perform(
      Task task, TaskActionToolbox toolbox
  )
  {
    final Optional<Task> maybeSurrogateTask = toolbox.getTaskStorage().getTask(surrogateId);
    if (maybeSurrogateTask.isPresent()) {
      final LockResult result = toolbox.getTaskLockbox().tryLock(type, maybeSurrogateTask.get(), interval);
      return result.isOk() ? result.getTaskLock() : null;
    } else {
      throw new ISE("Can't find surrogate task[%s]", surrogateId);
    }
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
           ", surrogateId=" + surrogateId +
           '}';
  }
}
