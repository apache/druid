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
public class TotalRetryLimitExceededFault extends BaseMSQFault
{
  static final String CODE = "TotalRetryLimitExceededFault";


  private final int maxRetryCount;

  private final String taskId;

  private final int retryCount;


  private final String rootErrorMessage;

  @JsonCreator
  public TotalRetryLimitExceededFault(
      @JsonProperty("maxRetryCount") int maxRetryCount,
      @JsonProperty("retryCount") int retryCount,
      @JsonProperty("taskId") String taskId,
      @JsonProperty("rootErrorMessage") String rootErrorMessage
  )
  {
    super(
        CODE,
        "Retry count %d exceeded total retry limit %d .Latest task[%s] failure reason: %s",
        retryCount,
        maxRetryCount,
        taskId,
        rootErrorMessage
    );
    this.maxRetryCount = maxRetryCount;
    this.retryCount = retryCount;
    this.taskId = taskId;

    this.rootErrorMessage = rootErrorMessage;
  }

  @JsonProperty
  public int getMaxRetryCount()
  {
    return maxRetryCount;
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  public int getRetryCount()
  {
    return retryCount;
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
    TotalRetryLimitExceededFault that = (TotalRetryLimitExceededFault) o;
    return maxRetryCount == that.maxRetryCount && retryCount == that.retryCount && Objects.equals(
        taskId,
        that.taskId
    ) && Objects.equals(rootErrorMessage, that.rootErrorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), maxRetryCount, taskId, retryCount, rootErrorMessage);
  }
}
