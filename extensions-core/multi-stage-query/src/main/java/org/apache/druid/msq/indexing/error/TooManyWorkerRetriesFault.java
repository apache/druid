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

@JsonTypeName(TotalRetryLimitExceededFault.CODE)
public class TooManyWorkerRetriesFault extends BaseMSQFault
{
  static final String CODE = "TooManyWorkerRetries";


  private final int maxRetryCount;

  private final String taskId;

  private final int workerNumber;


  private final String rootErrorMessage;

  @JsonCreator
  public TooManyWorkerRetriesFault(
      @JsonProperty("maxRetryCount") int maxRetryCount,
      @JsonProperty("taskId") String taskId,
      @JsonProperty("workerNumber") int workerNumber,
      @JsonProperty("rootErrorMessage") String rootErrorMessage
  )
  {
    super(
        CODE,
        "Worker[%d] exceeded max retry count of %d for task[%s]. Latest failure reason: %s.",
        workerNumber,
        maxRetryCount,
        taskId,
        rootErrorMessage
    );
    this.maxRetryCount = maxRetryCount;
    this.taskId = taskId;
    this.workerNumber = workerNumber;
    this.rootErrorMessage = rootErrorMessage;
  }

  @JsonProperty
  public int getMaxRetryCount()
  {
    return maxRetryCount;
  }

  @JsonProperty
  public int getWorkerNumber()
  {
    return workerNumber;
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
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
    TooManyWorkerRetriesFault that = (TooManyWorkerRetriesFault) o;
    return maxRetryCount == that.maxRetryCount && workerNumber == that.workerNumber && Objects.equals(
        taskId,
        that.taskId
    ) && Objects.equals(rootErrorMessage, that.rootErrorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), maxRetryCount, taskId, workerNumber, rootErrorMessage);
  }
}
