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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.kernel.StageId;

import java.util.Objects;

/**
 * Message for {@link ControllerClient#postDoneReadingInput}.
 */
public class DoneReadingInput implements ControllerMessage
{
  private final StageId stageId;
  private final int workerNumber;

  @JsonCreator
  public DoneReadingInput(
      @JsonProperty("stage") final StageId stageId,
      @JsonProperty("worker") final int workerNumber
  )
  {
    this.stageId = Preconditions.checkNotNull(stageId, "stageId");
    this.workerNumber = workerNumber;
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

  @Override
  public void handle(Controller controller)
  {
    controller.doneReadingInput(stageId.getStageNumber(), workerNumber);
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
    DoneReadingInput that = (DoneReadingInput) o;
    return workerNumber == that.workerNumber
           && Objects.equals(stageId, that.stageId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageId, workerNumber);
  }

  @Override
  public String toString()
  {
    return "DoneReadingInput{" +
           "stageId=" + stageId +
           ", workerNumber=" + workerNumber +
           '}';
  }
}
