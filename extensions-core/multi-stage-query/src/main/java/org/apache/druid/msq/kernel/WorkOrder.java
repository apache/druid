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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.input.InputSlice;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Represents the work done by a single worker in a single stage.
 *
 * A list of {@link InputSlice} provides the inputs that the worker reads. These are eventually passed to the
 * {@link FrameProcessorFactory#makeProcessors} method for the processor associated with the {@link StageDefinition}.
 *
 * The entire {@link QueryDefinition} is included, even for other stages, to enable the worker to correctly read
 * from the outputs of prior stages.
 */
public class WorkOrder
{
  private final QueryDefinition queryDefinition;
  private final int stageNumber;
  private final int workerNumber;
  private final List<InputSlice> workerInputs;
  private final ExtraInfoHolder<?> extraInfoHolder;

  @Nullable
  private final List<String> workerIds;

  @Nullable
  private final OutputChannelMode outputChannelMode;

  @JsonCreator
  @SuppressWarnings("rawtypes")
  public WorkOrder(
      @JsonProperty("query") final QueryDefinition queryDefinition,
      @JsonProperty("stage") final int stageNumber,
      @JsonProperty("worker") final int workerNumber,
      @JsonProperty("input") final List<InputSlice> workerInputs,
      @JsonProperty("extra") @Nullable final ExtraInfoHolder extraInfoHolder,
      @JsonProperty("workers") @Nullable final List<String> workerIds,
      @JsonProperty("output") @Nullable final OutputChannelMode outputChannelMode
  )
  {
    this.queryDefinition = Preconditions.checkNotNull(queryDefinition, "queryDefinition");
    this.stageNumber = stageNumber;
    this.workerNumber = workerNumber;
    this.workerInputs = Preconditions.checkNotNull(workerInputs, "workerInputs");
    this.extraInfoHolder = extraInfoHolder;
    this.workerIds = workerIds;
    this.outputChannelMode = outputChannelMode;
  }

  @JsonProperty("query")
  public QueryDefinition getQueryDefinition()
  {
    return queryDefinition;
  }

  @JsonProperty("stage")
  public int getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty("worker")
  public int getWorkerNumber()
  {
    return workerNumber;
  }

  @JsonProperty("input")
  public List<InputSlice> getInputs()
  {
    return workerInputs;
  }

  @Nullable
  @JsonProperty("extra")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  ExtraInfoHolder<?> getExtraInfoHolder()
  {
    return extraInfoHolder;
  }

  /**
   * Worker IDs for this query, if known in advance (at the time the work order is created). May be null, in which
   * case workers use {@link ControllerClient#getTaskList()} to find worker IDs.
   */
  @Nullable
  @JsonProperty("workers")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getWorkerIds()
  {
    return workerIds;
  }

  public boolean hasOutputChannelMode()
  {
    return outputChannelMode != null;
  }

  @Nullable
  @JsonProperty("output")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public OutputChannelMode getOutputChannelMode()
  {
    return outputChannelMode;
  }

  @Nullable
  public Object getExtraInfo()
  {
    return extraInfoHolder != null ? extraInfoHolder.getExtraInfo() : null;
  }

  public StageDefinition getStageDefinition()
  {
    return queryDefinition.getStageDefinition(stageNumber);
  }

  public WorkOrder withOutputChannelMode(final OutputChannelMode newOutputChannelMode)
  {
    if (newOutputChannelMode == outputChannelMode) {
      return this;
    } else {
      return new WorkOrder(
          queryDefinition,
          stageNumber,
          workerNumber,
          workerInputs,
          extraInfoHolder,
          workerIds,
          newOutputChannelMode
      );
    }
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
    WorkOrder workOrder = (WorkOrder) o;
    return stageNumber == workOrder.stageNumber
           && workerNumber == workOrder.workerNumber
           && Objects.equals(queryDefinition, workOrder.queryDefinition)
           && Objects.equals(workerInputs, workOrder.workerInputs)
           && Objects.equals(extraInfoHolder, workOrder.extraInfoHolder)
           && Objects.equals(workerIds, workOrder.workerIds)
           && Objects.equals(outputChannelMode, workOrder.outputChannelMode);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        queryDefinition,
        stageNumber,
        workerNumber,
        workerInputs,
        extraInfoHolder,
        workerIds,
        outputChannelMode
    );
  }

  @Override
  public String toString()
  {
    return "WorkOrder{" +
           "queryDefinition=" + queryDefinition +
           ", stageNumber=" + stageNumber +
           ", workerNumber=" + workerNumber +
           ", workerInputs=" + workerInputs +
           ", extraInfoHolder=" + extraInfoHolder +
           ", workerIds=" + workerIds +
           ", outputChannelMode=" + outputChannelMode +
           '}';
  }
}
