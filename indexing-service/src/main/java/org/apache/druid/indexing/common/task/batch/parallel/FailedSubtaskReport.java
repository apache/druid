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
import org.apache.druid.indexing.stats.NoopIngestionMetricsSnapshot;

import java.util.Objects;

/**
 * A report sent when a subtask fails. This report doesn't include the exception thrown in the failed subtask.
 * The exception of the failed task should be stored in {@link org.apache.druid.indexer.TaskStatus#errorMsg} and
 * propagated to the Overlord via task status tracking framework.
 */
public class FailedSubtaskReport implements SubTaskReport
{
  public static final String TYPE = "failed";

  private final long createdTimeNs;
  private final String taskId;

  public FailedSubtaskReport(String taskId)
  {
    this(System.nanoTime(), taskId);
  }

  @JsonCreator
  private FailedSubtaskReport(@JsonProperty("createdTimeNs") long createdTimeNs, @JsonProperty("taskId") String taskId)
  {
    this.createdTimeNs = createdTimeNs;
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
  }

  @Override
  public long getCreatedTimeNs()
  {
    return createdTimeNs;
  }

  @JsonProperty
  @Override
  public String getTaskId()
  {
    return taskId;
  }

  @Override
  public TaskState getState()
  {
    return TaskState.FAILED;
  }

  @Override
  public IngestionMetricsSnapshot getMetrics()
  {
    return NoopIngestionMetricsSnapshot.INSTANCE;
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
    FailedSubtaskReport report = (FailedSubtaskReport) o;
    return createdTimeNs == report.createdTimeNs &&
           Objects.equals(taskId, report.taskId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(createdTimeNs, taskId);
  }
}
