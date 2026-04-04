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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.TaskState;

import javax.annotation.Nullable;
import java.util.Objects;

public class WorkerStats
{
  private final String workerId;
  @Nullable
  private final String workerDesc;
  private final TaskState state;
  private final long durationMs;
  private final long pendingMs;

  @JsonCreator
  public WorkerStats(
      @JsonProperty("workerId") final String workerId,
      @JsonProperty("workerDesc") @Nullable final String workerDesc,
      @JsonProperty("state") final TaskState state,
      @JsonProperty("durationMs") final long durationMs,
      @JsonProperty("pendingMs") final long pendingMs
  )
  {
    this.workerId = workerId;
    this.workerDesc = workerDesc;
    this.state = state;
    this.durationMs = durationMs;
    this.pendingMs = pendingMs;
  }

  /**
   * Unique worker ID, same as {@link Worker#id()}.
   */
  @JsonProperty
  public String getWorkerId()
  {
    return workerId;
  }

  /**
   * Worker description. Used by the web console to more easily find where a worker is running. Generally this is
   * either a task ID or a host:port. It may not be unique; for example, with Dart, if multiple queries run on the
   * same host:port then they would have unique workerId but would have the same workerDesc.
   */
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getWorkerDesc()
  {
    return workerDesc;
  }

  @JsonProperty
  public TaskState getState()
  {
    return state;
  }

  @JsonProperty("durationMs")
  public long getDuration()
  {
    return durationMs;
  }

  @JsonProperty("pendingMs")
  public long getPendingTimeInMs()
  {
    return pendingMs;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerStats that = (WorkerStats) o;
    return durationMs == that.durationMs
           && pendingMs == that.pendingMs
           && Objects.equals(workerId, that.workerId)
           && Objects.equals(workerDesc, that.workerDesc)
           && state == that.state;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(workerId, workerDesc, state, durationMs, pendingMs);
  }

  @Override
  public String toString()
  {
    return "WorkerStats{" +
           "workerId='" + workerId + '\'' +
           ", workerDesc='" + workerDesc + '\'' +
           ", state=" + state +
           ", durationMs=" + durationMs +
           ", pendingMs=" + pendingMs +
           '}';
  }
}
