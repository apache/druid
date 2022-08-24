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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(WorkerFailedFault.CODE)
public class WorkerFailedFault extends BaseMSQFault
{
  public static final String CODE = "WorkerFailed";

  private final String workerTaskId;

  @Nullable
  private final String errorMsg;

  @JsonCreator
  public WorkerFailedFault(
      @JsonProperty("workerTaskId") final String workerTaskId,
      @JsonProperty("errorMsg") @Nullable final String errorMsg
  )
  {
    super(CODE, "Worker task failed: [%s]%s", workerTaskId, errorMsg != null ? " (" + errorMsg + ")" : "");
    this.workerTaskId = workerTaskId;
    this.errorMsg = errorMsg;
  }

  @JsonProperty
  public String getWorkerTaskId()
  {
    return workerTaskId;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getErrorMsg()
  {
    return errorMsg;
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
    WorkerFailedFault that = (WorkerFailedFault) o;
    return Objects.equals(workerTaskId, that.workerTaskId) && Objects.equals(errorMsg, that.errorMsg);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), workerTaskId, errorMsg);
  }
}
