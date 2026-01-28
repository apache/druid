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
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;

import java.util.Objects;

/**
 * Message for {@link ControllerClient#postPartialKeyStatistics}.
 */
public class PartialKeyStatistics implements ControllerMessage
{
  private final StageId stageId;
  private final int workerNumber;
  private final PartialKeyStatisticsInformation payload;

  @JsonCreator
  public PartialKeyStatistics(
      @JsonProperty("stage") final StageId stageId,
      @JsonProperty("worker") final int workerNumber,
      @JsonProperty("payload") final PartialKeyStatisticsInformation payload
  )
  {
    this.stageId = Preconditions.checkNotNull(stageId, "stageId");
    this.workerNumber = workerNumber;
    this.payload = payload;
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

  @JsonProperty
  public PartialKeyStatisticsInformation getPayload()
  {
    return payload;
  }


  @Override
  public void handle(Controller controller)
  {
    controller.updatePartialKeyStatisticsInformation(
        stageId.getStageNumber(),
        workerNumber,
        payload
    );
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
    PartialKeyStatistics that = (PartialKeyStatistics) o;
    return workerNumber == that.workerNumber
           && Objects.equals(stageId, that.stageId)
           && Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageId, workerNumber, payload);
  }

  @Override
  public String toString()
  {
    return "PartialKeyStatistics{" +
           "stageId=" + stageId +
           ", workerNumber=" + workerNumber +
           ", payload=" + payload +
           '}';
  }
}
