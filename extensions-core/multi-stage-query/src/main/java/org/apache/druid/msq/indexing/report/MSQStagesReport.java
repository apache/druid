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

package org.apache.druid.msq.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.controller.ControllerStagePhase;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MSQStagesReport
{
  private final List<Stage> stages;

  @JsonCreator
  public MSQStagesReport(final List<Stage> stages)
  {
    this.stages = Preconditions.checkNotNull(stages, "stages");
  }

  public static MSQStagesReport create(
      final QueryDefinition queryDef,
      final Map<Integer, ControllerStagePhase> stagePhaseMap,
      final Map<Integer, Interval> stageRuntimeMap,
      final Map<Integer, Integer> stageWorkerCountMap,
      final Map<Integer, Integer> stagePartitionCountMap
  )
  {
    final List<Stage> stages = new ArrayList<>();

    final int[] stageNumbers =
        queryDef.getStageDefinitions().stream().mapToInt(StageDefinition::getStageNumber).toArray();
    Arrays.sort(stageNumbers);

    for (final int stageNumber : stageNumbers) {
      final StageDefinition stageDef = queryDef.getStageDefinition(stageNumber);

      final int workerCount = stageWorkerCountMap.getOrDefault(stageNumber, 0);
      final int partitionCount = stagePartitionCountMap.getOrDefault(stageNumber, 0);
      final Interval stageRuntimeInterval = stageRuntimeMap.get(stageNumber);
      final DateTime stageStartTime = stageRuntimeInterval == null ? null : stageRuntimeInterval.getStart();
      final long stageDuration = stageRuntimeInterval == null ? 0 : stageRuntimeInterval.toDurationMillis();

      final Stage stage = new Stage(
          stageNumber,
          stageDef,
          stagePhaseMap.get(stageNumber),
          workerCount,
          partitionCount,
          stageStartTime,
          stageDuration
      );
      stages.add(stage);
    }

    return new MSQStagesReport(stages);
  }

  @JsonValue
  public List<Stage> getStages()
  {
    return stages;
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
    MSQStagesReport that = (MSQStagesReport) o;
    return Objects.equals(stages, that.stages);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stages);
  }

  @Override
  public String toString()
  {
    return "MSQStagesReport{" +
           "stages=" + stages +
           '}';
  }

  public static class Stage
  {
    private final int stageNumber;
    private final StageDefinition stageDef;
    @Nullable
    private final ControllerStagePhase phase;
    private final int workerCount;
    private final int partitionCount;
    private final DateTime startTime;
    private final long duration;

    @JsonCreator
    private Stage(
        @JsonProperty("stageNumber") final int stageNumber,
        @JsonProperty("definition") final StageDefinition stageDef,
        @JsonProperty("phase") @Nullable final ControllerStagePhase phase,
        @JsonProperty("workerCount") final int workerCount,
        @JsonProperty("partitionCount") final int partitionCount,
        @JsonProperty("startTime") @Nullable final DateTime startTime,
        @JsonProperty("duration") final long duration
    )
    {
      this.stageNumber = stageNumber;
      this.stageDef = stageDef;
      this.phase = phase;
      this.workerCount = workerCount;
      this.partitionCount = partitionCount;
      this.startTime = startTime;
      this.duration = duration;
    }

    @JsonProperty
    public int getStageNumber()
    {
      return stageNumber;
    }

    @JsonProperty("definition")
    public StageDefinition getStageDefinition()
    {
      return stageDef;
    }

    @Nullable
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ControllerStagePhase getPhase()
    {
      // Null if the stage has not yet been started.
      return phase;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getWorkerCount()
    {
      return workerCount;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getPartitionCount()
    {
      return partitionCount;
    }

    @JsonProperty("sort")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isSorting()
    {
      // Field written out, but not read, because it is derived from "definition".
      return stageDef.doesSortDuringShuffle();
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public DateTime getStartTime()
    {
      return startTime;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long getDuration()
    {
      return duration;
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
      Stage stage = (Stage) o;
      return stageNumber == stage.stageNumber
             && workerCount == stage.workerCount
             && partitionCount == stage.partitionCount
             && duration == stage.duration
             && Objects.equals(stageDef, stage.stageDef)
             && phase == stage.phase
             && Objects.equals(startTime, stage.startTime);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(stageNumber, stageDef, phase, workerCount, partitionCount, startTime, duration);
    }

    @Override
    public String toString()
    {
      return "Stage{" +
             "stageNumber=" + stageNumber +
             ", stageDef=" + stageDef +
             ", phase=" + phase +
             ", workerCount=" + workerCount +
             ", partitionCount=" + partitionCount +
             ", startTime=" + startTime +
             ", duration=" + duration +
             '}';
    }
  }
}
