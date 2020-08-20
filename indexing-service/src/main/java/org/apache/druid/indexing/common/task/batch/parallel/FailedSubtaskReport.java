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
import org.apache.druid.indexer.TaskState;

public class FailedSubtaskReport implements SubTaskReport
{
  private final String taskId;
  private final String error;
  private final long duration;

  @JsonCreator
  public FailedSubtaskReport(
      @JsonProperty("taskId") String taskId,
      @JsonProperty("error") String error,
      @JsonProperty("duration") long duration
  )
  {
    this.taskId = taskId;
    this.error = error;
    this.duration = duration;
  }

  @JsonProperty
  @Override
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  public String getError()
  {
    return error;
  }

  @JsonProperty
  public long getDuration()
  {
    return duration;
  }

  @Override
  public TaskState getState()
  {
    return TaskState.FAILED;
  }
}
