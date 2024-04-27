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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName(TooManyAttemptsForJob.CODE)
public class TooManyAttemptsForJob extends BaseMSQFault
{
  static final String CODE = "TooManyAttemptsForJob";


  private final int maxRelaunchCount;

  private final String taskId;

  private final int currentRelaunchCount;


  private final String rootErrorMessage;

  @JsonCreator
  public TooManyAttemptsForJob(
      @JsonProperty("maxRelaunchCount") int maxRelaunchCount,
      @JsonProperty("currentRelaunchCount") int currentRelaunchCount,
      @JsonProperty("taskId") String taskId,
      @JsonProperty("rootErrorMessage") String rootErrorMessage
  )
  {
    super(
        CODE,
        "Total relaunch count across all workers %d exceeded max relaunch limit %d . Latest task[%s] failure reason: %s",
        currentRelaunchCount,
        maxRelaunchCount,
        taskId,
        rootErrorMessage
    );
    this.maxRelaunchCount = maxRelaunchCount;
    this.currentRelaunchCount = currentRelaunchCount;
    this.taskId = taskId;

    this.rootErrorMessage = rootErrorMessage;
  }

  @JsonProperty
  public int getMaxRelaunchCount()
  {
    return maxRelaunchCount;
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  public int getCurrentRelaunchCount()
  {
    return currentRelaunchCount;
  }

  @JsonProperty
  public String getRootErrorMessage()
  {
    return rootErrorMessage;
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
    if (!super.equals(o)) {
      return false;
    }
    TooManyAttemptsForJob that = (TooManyAttemptsForJob) o;
    return maxRelaunchCount == that.maxRelaunchCount
           && currentRelaunchCount == that.currentRelaunchCount
           && Objects.equals(
        taskId,
        that.taskId
    )
           && Objects.equals(rootErrorMessage, that.rootErrorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), maxRelaunchCount, taskId, currentRelaunchCount, rootErrorMessage);
  }
}
