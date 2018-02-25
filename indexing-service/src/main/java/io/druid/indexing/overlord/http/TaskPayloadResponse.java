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

package io.druid.indexing.overlord.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.common.task.Task;

import java.util.Objects;

public class TaskPayloadResponse
{
  private final String task; // Task ID, named "task" in the JSONification of this class.
  private final Task payload;

  @JsonCreator
  public TaskPayloadResponse(
      @JsonProperty("task") final String task,
      @JsonProperty("payload") final Task payload
  )
  {
    this.task = task;
    this.payload = payload;
  }

  @JsonProperty
  public String getTask()
  {
    return task;
  }

  @JsonProperty
  public Task getPayload()
  {
    return payload;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TaskPayloadResponse that = (TaskPayloadResponse) o;
    return Objects.equals(task, that.task) &&
           Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(task, payload);
  }

  @Override
  public String toString()
  {
    return "TaskPayloadResponse{" +
           "task='" + task + '\'' +
           ", payload=" + payload +
           '}';
  }
}
