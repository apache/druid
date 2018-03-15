/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.segment.IndexSpec;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName("parallelIndexSinglePhase")
public class ParallelIndexSinglePhaseTuningConfig extends IndexTuningConfig
{
  private static final int DEFAULT_MAX_NUM_BATCH_TASKS = Integer.MAX_VALUE; // unlimited
  private static final int DEFAULT_MAX_RETRY = 3;
  private static final long DEFAULT_TASK_STATUS_CHECKING_PERIOD_MS = 1000;

  private final int maxNumBatchTasks;
  private final int maxRetry;
  private final long taskStatusCheckingPeriodMs;

  public static ParallelIndexSinglePhaseTuningConfig defaultConfig()
  {
    return new ParallelIndexSinglePhaseTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @JsonCreator
  public ParallelIndexSinglePhaseTuningConfig(
      @JsonProperty("targetPartitionSize") @Nullable Integer targetPartitionSize,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("rowFlushBoundary") @Nullable Integer rowFlushBoundary_forBackCompatibility, // DEPRECATED
      @JsonProperty("numShards") @Nullable Integer numShards,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      // This parameter is left for compatibility when reading existing JSONs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
      @JsonProperty("forceExtendableShardSpecs") @Nullable Boolean forceExtendableShardSpecs,
      @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
      @JsonProperty("publishTimeout") @Nullable Long publishTimeout, // deprecated
      @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("maxNumBatchTasks") @Nullable Integer maxNumBatchTasks,
      @JsonProperty("maxRetry") @Nullable Integer maxRetry,
      @JsonProperty("taskStatusCheckingPeriodMs") @Nullable Integer taskStatusCheckingPeriodMs
  )
  {
    super(
        targetPartitionSize,
        maxRowsInMemory,
        maxTotalRows,
        rowFlushBoundary_forBackCompatibility,
        numShards,
        indexSpec,
        maxPendingPersists,
        buildV9Directly,
        forceExtendableShardSpecs,
        false, // ParallelIndexSinglePhaseSupervisorTask can't be used for guaranteed rollup
        reportParseExceptions,
        publishTimeout,
        pushTimeout,
        segmentWriteOutMediumFactory
    );

    this.maxNumBatchTasks = maxNumBatchTasks == null ? DEFAULT_MAX_NUM_BATCH_TASKS : maxNumBatchTasks;
    this.maxRetry = maxRetry == null ? DEFAULT_MAX_RETRY : maxRetry;
    this.taskStatusCheckingPeriodMs = taskStatusCheckingPeriodMs == null ?
                                      DEFAULT_TASK_STATUS_CHECKING_PERIOD_MS :
                                      taskStatusCheckingPeriodMs;
  }

  @JsonProperty
  public int getMaxNumBatchTasks()
  {
    return maxNumBatchTasks;
  }

  @JsonProperty
  public int getMaxRetry()
  {
    return maxRetry;
  }

  @JsonProperty
  public long getTaskStatusCheckingPeriodMs()
  {
    return taskStatusCheckingPeriodMs;
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
    ParallelIndexSinglePhaseTuningConfig that = (ParallelIndexSinglePhaseTuningConfig) o;
    return getMaxRowsInMemory() == that.getMaxRowsInMemory() &&
           Objects.equals(getMaxTotalRows(), that.getMaxTotalRows()) &&
           getMaxPendingPersists() == that.getMaxPendingPersists() &&
           isForceExtendableShardSpecs() == that.isForceExtendableShardSpecs() &&
           isForceGuaranteedRollup() == that.isForceGuaranteedRollup() &&
           isReportParseExceptions() == that.isReportParseExceptions() &&
           getPushTimeout() == that.getPushTimeout() &&
           Objects.equals(getTargetPartitionSize(), that.getTargetPartitionSize()) &&
           Objects.equals(getNumShards(), that.getNumShards()) &&
           Objects.equals(getIndexSpec(), that.getIndexSpec()) &&
           Objects.equals(getBasePersistDirectory(), that.getBasePersistDirectory()) &&
           Objects.equals(getSegmentWriteOutMediumFactory(), that.getSegmentWriteOutMediumFactory()) &&
           maxNumBatchTasks == that.maxNumBatchTasks &&
           maxRetry == that.maxRetry &&
           taskStatusCheckingPeriodMs == that.taskStatusCheckingPeriodMs;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getTargetPartitionSize(),
        getMaxRowsInMemory(),
        getMaxTotalRows(),
        getNumShards(),
        getIndexSpec(),
        getBasePersistDirectory(),
        getMaxPendingPersists(),
        isForceExtendableShardSpecs(),
        isForceGuaranteedRollup(),
        isReportParseExceptions(),
        getPushTimeout(),
        getSegmentWriteOutMediumFactory(),
        maxNumBatchTasks,
        maxRetry,
        taskStatusCheckingPeriodMs
    );
  }
}
