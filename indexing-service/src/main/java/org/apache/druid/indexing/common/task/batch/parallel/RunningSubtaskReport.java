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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.stats.IngestionMetricsSnapshot;

import java.util.Objects;

public class RunningSubtaskReport implements SubTaskReport
{
  public static final String TYPE = "running";

  private final long createdTimeNs;
  private final String subtaskId;
  private final IngestionMetricsSnapshot metrics;

  public RunningSubtaskReport(String subtaskId, IngestionMetricsSnapshot metrics)
  {
    this(System.nanoTime(), subtaskId, metrics);
  }

  @JsonCreator
  private RunningSubtaskReport(
      @JsonProperty("createdTimeNs") long createdTimeNs,
      @JsonProperty("taskId") String subtaskId,
      @JsonProperty("metrics") IngestionMetricsSnapshot metrics
  )
  {
    this.createdTimeNs = createdTimeNs;
    this.subtaskId = Preconditions.checkNotNull(subtaskId, "subtaskId");
    this.metrics = Preconditions.checkNotNull(metrics, "metrics");
  }

  @Override
  @JsonProperty
  public long getCreatedTimeNs()
  {
    return createdTimeNs;
  }

  @JsonProperty
  @Override
  public String getTaskId()
  {
    return subtaskId;
  }

  @Override
  public TaskState getState()
  {
    return TaskState.RUNNING;
  }

  @JsonProperty
  @Override
  public IngestionMetricsSnapshot getMetrics()
  {
    return metrics;
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
    RunningSubtaskReport that = (RunningSubtaskReport) o;
    return createdTimeNs == that.createdTimeNs &&
           Objects.equals(subtaskId, that.subtaskId) &&
           Objects.equals(metrics, that.metrics);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(createdTimeNs, subtaskId, metrics);
  }
}
