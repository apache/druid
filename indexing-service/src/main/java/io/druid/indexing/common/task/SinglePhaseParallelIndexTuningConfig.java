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
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName("index_single_phase_parallel")
public class SinglePhaseParallelIndexTuningConfig extends IndexTuningConfig
{
  private static final int DEFAULT_MAX_NUM_BATCH_TASKS = Integer.MAX_VALUE; // unlimited
  private static final int DEFAULT_MAX_RETRY = 3;
  private static final long DEFAULT_TASK_STATUS_CHECK_PERIOD_MS = 1000;

  private static final Duration DEFAULT_CHAT_HANDLER_TIMEOUT = new Period("PT10S").toStandardDuration();
  private static final int DEFAULT_CHAT_HANDLER_NUM_RETRIES = 5;

  private final int maxNumBatchTasks;
  private final int maxRetry;
  private final long taskStatusCheckPeriodMs;

  private final Duration chatHandlerTimeout;
  private final int chatHandlerNumRetries;

  public static SinglePhaseParallelIndexTuningConfig defaultConfig()
  {
    return new SinglePhaseParallelIndexTuningConfig(
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
        null,
        null,
        null
    );
  }

  @JsonCreator
  public SinglePhaseParallelIndexTuningConfig(
      @JsonProperty("targetPartitionSize") @Nullable Integer targetPartitionSize,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("numShards") @Nullable Integer numShards,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("forceExtendableShardSpecs") @Nullable Boolean forceExtendableShardSpecs,
      @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
      @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("maxNumBatchTasks") @Nullable Integer maxNumBatchTasks,
      @JsonProperty("maxRetry") @Nullable Integer maxRetry,
      @JsonProperty("taskStatusCheckPeriodMs") @Nullable Integer taskStatusCheckPeriodMs,
      @JsonProperty("chatHandlerTimeout") @Nullable Duration chatHandlerTimeout,
      @JsonProperty("chatHandlerNumRetries") @Nullable Integer chatHandlerNumRetries,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
  )
  {
    super(
        targetPartitionSize,
        maxRowsInMemory,
        maxTotalRows,
        null,
        numShards,
        indexSpec,
        maxPendingPersists,
        null,
        forceExtendableShardSpecs,
        false, // SinglePhaseParallelIndexSupervisorTask can't be used for guaranteed rollup
        reportParseExceptions,
        null,
        pushTimeout,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );

    this.maxNumBatchTasks = maxNumBatchTasks == null ? DEFAULT_MAX_NUM_BATCH_TASKS : maxNumBatchTasks;
    this.maxRetry = maxRetry == null ? DEFAULT_MAX_RETRY : maxRetry;
    this.taskStatusCheckPeriodMs = taskStatusCheckPeriodMs == null ?
                                   DEFAULT_TASK_STATUS_CHECK_PERIOD_MS :
                                   taskStatusCheckPeriodMs;

    this.chatHandlerTimeout = DEFAULT_CHAT_HANDLER_TIMEOUT;
    this.chatHandlerNumRetries = DEFAULT_CHAT_HANDLER_NUM_RETRIES;
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
  public long getTaskStatusCheckPeriodMs()
  {
    return taskStatusCheckPeriodMs;
  }

  @JsonProperty
  public Duration getChatHandlerTimeout()
  {
    return chatHandlerTimeout;
  }

  @JsonProperty
  public int getChatHandlerNumRetries()
  {
    return chatHandlerNumRetries;
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
    SinglePhaseParallelIndexTuningConfig that = (SinglePhaseParallelIndexTuningConfig) o;
    return maxNumBatchTasks == that.maxNumBatchTasks &&
           maxRetry == that.maxRetry &&
           taskStatusCheckPeriodMs == that.taskStatusCheckPeriodMs &&
           chatHandlerNumRetries == that.chatHandlerNumRetries &&
           Objects.equals(chatHandlerTimeout, that.chatHandlerTimeout);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(
        super.hashCode(),
        maxNumBatchTasks,
        maxRetry,
        taskStatusCheckPeriodMs,
        chatHandlerTimeout,
        chatHandlerNumRetries
    );
  }
}
