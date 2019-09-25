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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName("index_parallel")
public class ParallelIndexTuningConfig extends IndexTuningConfig
{
  private static final int DEFAULT_MAX_NUM_CONCURRENT_SUB_TASKS = 1;
  private static final int DEFAULT_MAX_RETRY = 3;
  private static final long DEFAULT_TASK_STATUS_CHECK_PERIOD_MS = 1000;

  private static final Duration DEFAULT_CHAT_HANDLER_TIMEOUT = new Period("PT10S").toStandardDuration();
  private static final int DEFAULT_CHAT_HANDLER_NUM_RETRIES = 5;
  private static final int DEFAULT_MAX_NUM_SEGMENTS_TO_MERGE = 100;
  private static final int DEFAULT_TOTAL_NUM_MERGE_TASKS = 10;

  private final int maxNumConcurrentSubTasks;
  private final int maxRetry;
  private final long taskStatusCheckPeriodMs;

  private final Duration chatHandlerTimeout;
  private final int chatHandlerNumRetries;

  /**
   * Max number of segments to merge at the same time.
   * Used only by {@link PartialSegmentMergeTask}.
   * This configuration was temporarily added to avoid using too much memory while merging segments,
   * and will be removed once {@link org.apache.druid.segment.IndexMerger} is improved to not use much memory.
   */
  private final int maxNumSegmentsToMerge;

  /**
   * Total number of tasks for partial segment merge (that is, number of {@link PartialSegmentMergeTask}s).
   * Used only when this task runs with shuffle.
   */
  private final int totalNumMergeTasks;

  static ParallelIndexTuningConfig defaultConfig()
  {
    return new ParallelIndexTuningConfig(
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
  public ParallelIndexTuningConfig(
      @JsonProperty("targetPartitionSize") @Deprecated @Nullable Integer targetPartitionSize,
      @JsonProperty("maxRowsPerSegment") @Deprecated @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("maxTotalRows") @Deprecated @Nullable Long maxTotalRows,
      @JsonProperty("numShards") @Deprecated @Nullable Integer numShards,
      @JsonProperty("partitionsSpec") @Nullable PartitionsSpec partitionsSpec,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("forceGuaranteedRollup") @Nullable Boolean forceGuaranteedRollup,
      @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
      @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("maxNumSubTasks") @Deprecated @Nullable Integer maxNumSubTasks,
      @JsonProperty("maxNumConcurrentSubTasks") @Nullable Integer maxNumConcurrentSubTasks,
      @JsonProperty("maxRetry") @Nullable Integer maxRetry,
      @JsonProperty("taskStatusCheckPeriodMs") @Nullable Integer taskStatusCheckPeriodMs,
      @JsonProperty("chatHandlerTimeout") @Nullable Duration chatHandlerTimeout,
      @JsonProperty("chatHandlerNumRetries") @Nullable Integer chatHandlerNumRetries,
      @JsonProperty("maxNumSegmentsToMerge") @Nullable Integer maxNumSegmentsToMerge,
      @JsonProperty("totalNumMergeTasks") @Nullable Integer totalNumMergeTasks,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
  )
  {
    super(
        targetPartitionSize,
        maxRowsPerSegment,
        maxRowsInMemory,
        maxBytesInMemory,
        maxTotalRows,
        null,
        numShards,
        null,
        partitionsSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        maxPendingPersists,
        forceGuaranteedRollup,
        reportParseExceptions,
        null,
        pushTimeout,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );

    if (maxNumSubTasks != null && maxNumConcurrentSubTasks != null) {
      throw new IAE("Can't use both maxNumSubTasks and maxNumConcurrentSubTasks. Use maxNumConcurrentSubTasks instead");
    }

    if (maxNumConcurrentSubTasks == null) {
      this.maxNumConcurrentSubTasks = maxNumSubTasks == null ? DEFAULT_MAX_NUM_CONCURRENT_SUB_TASKS : maxNumSubTasks;
    } else {
      this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
    }
    this.maxRetry = maxRetry == null ? DEFAULT_MAX_RETRY : maxRetry;
    this.taskStatusCheckPeriodMs = taskStatusCheckPeriodMs == null ?
                                   DEFAULT_TASK_STATUS_CHECK_PERIOD_MS :
                                   taskStatusCheckPeriodMs;

    this.chatHandlerTimeout = chatHandlerTimeout == null ? DEFAULT_CHAT_HANDLER_TIMEOUT : chatHandlerTimeout;
    this.chatHandlerNumRetries = chatHandlerNumRetries == null
                                 ? DEFAULT_CHAT_HANDLER_NUM_RETRIES
                                 : chatHandlerNumRetries;

    this.maxNumSegmentsToMerge = maxNumSegmentsToMerge == null
                                 ? DEFAULT_MAX_NUM_SEGMENTS_TO_MERGE
                                 : maxNumSegmentsToMerge;

    this.totalNumMergeTasks = totalNumMergeTasks == null
                            ? DEFAULT_TOTAL_NUM_MERGE_TASKS
                            : totalNumMergeTasks;

    Preconditions.checkArgument(this.maxNumConcurrentSubTasks > 0, "maxNumConcurrentSubTasks must be positive");
    Preconditions.checkArgument(this.maxNumSegmentsToMerge > 0, "maxNumSegmentsToMerge must be positive");
    Preconditions.checkArgument(this.totalNumMergeTasks > 0, "totalNumMergeTasks must be positive");
  }

  @JsonProperty
  public int getMaxNumConcurrentSubTasks()
  {
    return maxNumConcurrentSubTasks;
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

  @JsonProperty
  public int getMaxNumSegmentsToMerge()
  {
    return maxNumSegmentsToMerge;
  }

  @JsonProperty
  public int getTotalNumMergeTasks()
  {
    return totalNumMergeTasks;
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
    ParallelIndexTuningConfig that = (ParallelIndexTuningConfig) o;
    return maxNumConcurrentSubTasks == that.maxNumConcurrentSubTasks &&
           maxRetry == that.maxRetry &&
           taskStatusCheckPeriodMs == that.taskStatusCheckPeriodMs &&
           chatHandlerNumRetries == that.chatHandlerNumRetries &&
           maxNumSegmentsToMerge == that.maxNumSegmentsToMerge &&
           totalNumMergeTasks == that.totalNumMergeTasks &&
           Objects.equals(chatHandlerTimeout, that.chatHandlerTimeout);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        maxNumConcurrentSubTasks,
        maxRetry,
        taskStatusCheckPeriodMs,
        chatHandlerTimeout,
        chatHandlerNumRetries,
        maxNumSegmentsToMerge,
        totalNumMergeTasks
    );
  }
}
