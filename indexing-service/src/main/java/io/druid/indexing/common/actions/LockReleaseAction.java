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
import io.druid.indexing.common.task.Task;
import org.joda.time.Interval;

public class LockReleaseAction implements TaskAction<Void>
{
  @JsonIgnore
  private final Interval interval;

  @JsonCreator
  public LockReleaseAction(
      @JsonProperty("interval") Interval interval
  )
  {
    this.interval = interval;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>() {};
  }

  @Override
  public Void perform(Task task, TaskActionToolbox toolbox)
  {
    toolbox.getTaskLockbox().unlock(task, interval);
    return null;
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "LockReleaseAction{" +
           "interval=" + interval +
           '}';
  }
}
