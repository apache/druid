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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Objects;

public class ClientCompactionTaskQueryTuningConfig
{
  @Deprecated
  @Nullable
  private final Integer maxRowsPerSegment;
  @Nullable
  private final Integer maxRowsInMemory;
  @Nullable
  private final Long maxBytesInMemory;
  @Deprecated
  @Nullable
  private final Long maxTotalRows;
  @Nullable
  private final SplitHintSpec splitHintSpec;
  @Nullable
  private final PartitionsSpec partitionsSpec;
  @Nullable
  private final IndexSpec indexSpec;
  @Nullable
  private final IndexSpec indexSpecForIntermediatePersists;
  @Nullable
  private final Integer maxPendingPersists;
  @Nullable
  private final Long pushTimeout;
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
  @Nullable
  private final Integer maxNumConcurrentSubTasks;
  @Nullable
  private final Integer maxRetry;
  @Nullable
  private final Long taskStatusCheckPeriodMs;
  @Nullable
  private final Duration chatHandlerTimeout;
  @Nullable
  private final Integer chatHandlerNumRetries;
  @Nullable
  private final Integer maxNumSegmentsToMerge;
  @Nullable
  private final Integer totalNumMergeTasks;
  @Nullable
  private final Integer maxColumnsToMerge;
  @Nullable
  private final AppendableIndexSpec appendableIndexSpec;

  public static ClientCompactionTaskQueryTuningConfig from(
      @Nullable UserCompactionTaskQueryTuningConfig userCompactionTaskQueryTuningConfig,
      @Nullable Integer maxRowsPerSegment,
      @Nullable Boolean preserveExistingMetrics
  )
  {
    if (userCompactionTaskQueryTuningConfig == null) {
      return new ClientCompactionTaskQueryTuningConfig(
          maxRowsPerSegment,
          new OnheapIncrementalIndex.Spec(preserveExistingMetrics),
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
    } else {
      AppendableIndexSpec appendableIndexSpecToUse = userCompactionTaskQueryTuningConfig.getAppendableIndexSpec() != null
                                                     ? userCompactionTaskQueryTuningConfig.getAppendableIndexSpec()
                                                     : new OnheapIncrementalIndex.Spec(preserveExistingMetrics);
      return new ClientCompactionTaskQueryTuningConfig(
          maxRowsPerSegment,
          appendableIndexSpecToUse,
          userCompactionTaskQueryTuningConfig.getMaxRowsInMemory(),
          userCompactionTaskQueryTuningConfig.getMaxBytesInMemory(),
          userCompactionTaskQueryTuningConfig.getMaxTotalRows(),
          userCompactionTaskQueryTuningConfig.getSplitHintSpec(),
          userCompactionTaskQueryTuningConfig.getPartitionsSpec(),
          userCompactionTaskQueryTuningConfig.getIndexSpec(),
          userCompactionTaskQueryTuningConfig.getIndexSpecForIntermediatePersists(),
          userCompactionTaskQueryTuningConfig.getMaxPendingPersists(),
          userCompactionTaskQueryTuningConfig.getPushTimeout(),
          userCompactionTaskQueryTuningConfig.getSegmentWriteOutMediumFactory(),
          userCompactionTaskQueryTuningConfig.getMaxNumConcurrentSubTasks(),
          userCompactionTaskQueryTuningConfig.getMaxRetry(),
          userCompactionTaskQueryTuningConfig.getTaskStatusCheckPeriodMs(),
          userCompactionTaskQueryTuningConfig.getChatHandlerTimeout(),
          userCompactionTaskQueryTuningConfig.getChatHandlerNumRetries(),
          userCompactionTaskQueryTuningConfig.getMaxNumSegmentsToMerge(),
          userCompactionTaskQueryTuningConfig.getTotalNumMergeTasks(),
          userCompactionTaskQueryTuningConfig.getMaxColumnsToMerge()
      );
    }
  }

  @JsonCreator
  public ClientCompactionTaskQueryTuningConfig(
      @JsonProperty("maxRowsPerSegment") @Deprecated @Nullable Integer maxRowsPerSegment,
      @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("maxTotalRows") @Deprecated @Nullable Long maxTotalRows,
      @JsonProperty("splitHintSpec") @Nullable SplitHintSpec splitHintSpec,
      @JsonProperty("partitionsSpec") @Nullable PartitionsSpec partitionsSpec,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("maxNumConcurrentSubTasks") @Nullable Integer maxNumConcurrentSubTasks,
      @JsonProperty("maxRetry") @Nullable Integer maxRetry,
      @JsonProperty("taskStatusCheckPeriodMs") @Nullable Long taskStatusCheckPeriodMs,
      @JsonProperty("chatHandlerTimeout") @Nullable Duration chatHandlerTimeout,
      @JsonProperty("chatHandlerNumRetries") @Nullable Integer chatHandlerNumRetries,
      @JsonProperty("maxNumSegmentsToMerge") @Nullable Integer maxNumSegmentsToMerge,
      @JsonProperty("totalNumMergeTasks") @Nullable Integer totalNumMergeTasks,
      @JsonProperty("maxColumnsToMerge") @Nullable Integer maxColumnsToMerge
  )
  {
    this.maxRowsPerSegment = maxRowsPerSegment;
    this.appendableIndexSpec = appendableIndexSpec;
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxBytesInMemory = maxBytesInMemory;
    this.maxTotalRows = maxTotalRows;
    this.splitHintSpec = splitHintSpec;
    this.partitionsSpec = partitionsSpec;
    this.indexSpec = indexSpec;
    this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists;
    this.maxPendingPersists = maxPendingPersists;
    this.pushTimeout = pushTimeout;
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
    this.maxRetry = maxRetry;
    this.taskStatusCheckPeriodMs = taskStatusCheckPeriodMs;
    this.chatHandlerTimeout = chatHandlerTimeout;
    this.chatHandlerNumRetries = chatHandlerNumRetries;
    this.maxNumSegmentsToMerge = maxNumSegmentsToMerge;
    this.totalNumMergeTasks = totalNumMergeTasks;
    this.maxColumnsToMerge = maxColumnsToMerge;
  }

  @JsonProperty
  public String getType()
  {
    return "index_parallel";
  }

  @Deprecated
  @JsonProperty
  @Nullable
  public Integer getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  @Nullable
  public Long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  @Deprecated
  @JsonProperty
  @Nullable
  public Long getMaxTotalRows()
  {
    return maxTotalRows;
  }

  @JsonProperty
  @Nullable
  public SplitHintSpec getSplitHintSpec()
  {
    return splitHintSpec;
  }

  @JsonProperty
  @Nullable
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @JsonProperty
  @Nullable
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  @Nullable
  public IndexSpec getIndexSpecForIntermediatePersists()
  {
    return indexSpecForIntermediatePersists;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @JsonProperty
  public boolean isForceGuaranteedRollup()
  {
    // Should be always true for non-dynamic partitionsSpec for now.
    return partitionsSpec != null && !(partitionsSpec instanceof DynamicPartitionsSpec);
  }

  @JsonProperty
  @Nullable
  public Long getPushTimeout()
  {
    return pushTimeout;
  }

  @JsonProperty
  @Nullable
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxNumConcurrentSubTasks()
  {
    return maxNumConcurrentSubTasks;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxRetry()
  {
    return maxRetry;
  }

  @JsonProperty
  @Nullable
  public Long getTaskStatusCheckPeriodMs()
  {
    return taskStatusCheckPeriodMs;
  }

  @JsonProperty
  @Nullable
  public Duration getChatHandlerTimeout()
  {
    return chatHandlerTimeout;
  }

  @JsonProperty
  @Nullable
  public Integer getChatHandlerNumRetries()
  {
    return chatHandlerNumRetries;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxNumSegmentsToMerge()
  {
    return maxNumSegmentsToMerge;
  }

  @JsonProperty
  @Nullable
  public Integer getTotalNumMergeTasks()
  {
    return totalNumMergeTasks;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxColumnsToMerge()
  {
    return maxColumnsToMerge;
  }

  @JsonProperty
  @Nullable
  public AppendableIndexSpec getAppendableIndexSpec()
  {
    return appendableIndexSpec;
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
    ClientCompactionTaskQueryTuningConfig that = (ClientCompactionTaskQueryTuningConfig) o;
    return Objects.equals(maxRowsPerSegment, that.maxRowsPerSegment) &&
           Objects.equals(maxRowsInMemory, that.maxRowsInMemory) &&
           Objects.equals(maxBytesInMemory, that.maxBytesInMemory) &&
           Objects.equals(maxTotalRows, that.maxTotalRows) &&
           Objects.equals(splitHintSpec, that.splitHintSpec) &&
           Objects.equals(partitionsSpec, that.partitionsSpec) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(indexSpecForIntermediatePersists, that.indexSpecForIntermediatePersists) &&
           Objects.equals(maxPendingPersists, that.maxPendingPersists) &&
           Objects.equals(pushTimeout, that.pushTimeout) &&
           Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory) &&
           Objects.equals(maxNumConcurrentSubTasks, that.maxNumConcurrentSubTasks) &&
           Objects.equals(maxRetry, that.maxRetry) &&
           Objects.equals(taskStatusCheckPeriodMs, that.taskStatusCheckPeriodMs) &&
           Objects.equals(chatHandlerTimeout, that.chatHandlerTimeout) &&
           Objects.equals(chatHandlerNumRetries, that.chatHandlerNumRetries) &&
           Objects.equals(maxNumSegmentsToMerge, that.maxNumSegmentsToMerge) &&
           Objects.equals(totalNumMergeTasks, that.totalNumMergeTasks) &&
           Objects.equals(maxColumnsToMerge, that.maxColumnsToMerge) &&
           Objects.equals(appendableIndexSpec, that.appendableIndexSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        maxRowsPerSegment,
        maxRowsInMemory,
        maxBytesInMemory,
        maxTotalRows,
        splitHintSpec,
        partitionsSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        maxPendingPersists,
        pushTimeout,
        segmentWriteOutMediumFactory,
        maxNumConcurrentSubTasks,
        maxRetry,
        taskStatusCheckPeriodMs,
        chatHandlerTimeout,
        chatHandlerNumRetries,
        maxNumSegmentsToMerge,
        totalNumMergeTasks,
        maxColumnsToMerge,
        appendableIndexSpec
    );
  }

  @Override
  public String toString()
  {
    return "ClientCompactionTaskQueryTuningConfig{" +
           "maxRowsPerSegment=" + maxRowsPerSegment +
           ", maxRowsInMemory=" + maxRowsInMemory +
           ", maxBytesInMemory=" + maxBytesInMemory +
           ", maxTotalRows=" + maxTotalRows +
           ", splitHintSpec=" + splitHintSpec +
           ", partitionsSpec=" + partitionsSpec +
           ", indexSpec=" + indexSpec +
           ", indexSpecForIntermediatePersists=" + indexSpecForIntermediatePersists +
           ", maxPendingPersists=" + maxPendingPersists +
           ", pushTimeout=" + pushTimeout +
           ", segmentWriteOutMediumFactory=" + segmentWriteOutMediumFactory +
           ", maxNumConcurrentSubTasks=" + maxNumConcurrentSubTasks +
           ", maxRetry=" + maxRetry +
           ", taskStatusCheckPeriodMs=" + taskStatusCheckPeriodMs +
           ", chatHandlerTimeout=" + chatHandlerTimeout +
           ", chatHandlerNumRetries=" + chatHandlerNumRetries +
           ", maxNumSegmentsToMerge=" + maxNumSegmentsToMerge +
           ", totalNumMergeTasks=" + totalNumMergeTasks +
           ", maxColumnsToMerge=" + maxColumnsToMerge +
           ", appendableIndexSpec=" + appendableIndexSpec +
           '}';
  }
}
