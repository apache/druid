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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.TaskState;

import java.util.Objects;

public class WorkerStats
{
  private final String workerId;
  private final TaskState state;
  private final long durationMs;
  private final long pendingMs;

  @JsonCreator
  public WorkerStats(
      @JsonProperty("workerId") String workerId,
      @JsonProperty("state") TaskState state,
      @JsonProperty("durationMs") long durationMs,
      @JsonProperty("pendingMs") long pendingMs
  )
  {
    this.workerId = workerId;
    this.state = state;
    this.durationMs = durationMs;
    this.pendingMs = pendingMs;
  }

  @JsonProperty
  public String getWorkerId()
  {
    return workerId;
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
           && state == that.state;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(workerId, state, durationMs, pendingMs);
  }

  @Override
  public String toString()
  {
    return "WorkerStats{" +
           "workerId='" + workerId + '\'' +
           ", state=" + state +
           ", durationMs=" + durationMs +
           ", pendingMs=" + pendingMs +
           '}';
  }
}
