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

package org.apache.druid.msq.dart.controller.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.kernel.StageId;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Message for {@link ControllerClient#postResultsComplete}.
 */
public class ResultsComplete implements ControllerMessage
{
  private final StageId stageId;
  private final int workerNumber;

  @Nullable
  private final Object resultObject;

  @JsonCreator
  public ResultsComplete(
      @JsonProperty("stage") final StageId stageId,
      @JsonProperty("worker") final int workerNumber,
      @Nullable @JsonProperty("result") final Object resultObject
  )
  {
    this.stageId = Preconditions.checkNotNull(stageId, "stageId");
    this.workerNumber = workerNumber;
    this.resultObject = resultObject;
  }

  @Override
  public String getQueryId()
  {
    return stageId.getQueryId();
  }

  @JsonProperty("stage")
  public StageId getStageId()
  {
    return stageId;
  }

  @JsonProperty("worker")
  public int getWorkerNumber()
  {
    return workerNumber;
  }

  @Nullable
  @JsonProperty("result")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Object getResultObject()
  {
    return resultObject;
  }

  @Override
  public void handle(Controller controller)
  {
    controller.resultsComplete(stageId.getQueryId(), stageId.getStageNumber(), workerNumber, resultObject);
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
    ResultsComplete that = (ResultsComplete) o;
    return workerNumber == that.workerNumber
           && Objects.equals(stageId, that.stageId)
           && Objects.equals(resultObject, that.resultObject);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageId, workerNumber, resultObject);
  }

  @Override
  public String toString()
  {
    return "ResultsComplete{" +
           "stageId=" + stageId +
           ", workerNumber=" + workerNumber +
           ", resultObject=" + resultObject +
           '}';
  }
}
