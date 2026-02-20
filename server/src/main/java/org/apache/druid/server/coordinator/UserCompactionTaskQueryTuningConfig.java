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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;

import javax.annotation.Nullable;

public class UserCompactionTaskQueryTuningConfig extends ClientCompactionTaskQueryTuningConfig
{
  @JsonCreator
  public UserCompactionTaskQueryTuningConfig(
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
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
    super(
        null,
        appendableIndexSpec,
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
        maxColumnsToMerge
    );
  }

  @Override
  @Nullable
  @JsonIgnore
  public Integer getMaxRowsPerSegment()
  {
    throw new UnsupportedOperationException();
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private Integer maxRowsInMemory;
    private AppendableIndexSpec appendableIndexSpec;
    private Long maxBytesInMemory;
    private Long maxTotalRows;
    private SplitHintSpec splitHintSpec;
    private PartitionsSpec partitionsSpec;
    private IndexSpec indexSpec;
    private IndexSpec indexSpecForIntermediatePersists;
    private Integer maxPendingPersists;
    private Long pushTimeout;
    private SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
    private Integer maxNumConcurrentSubTasks;
    private Integer maxRetry;
    private Long taskStatusCheckPeriodMs;
    private Duration chatHandlerTimeout;
    private Integer chatHandlerNumRetries;
    private Integer maxNumSegmentsToMerge;
    private Integer totalNumMergeTasks;
    private Integer maxColumnsToMerge;

    public Builder maxRowsInMemory(Integer maxRowsInMemory)
    {
      this.maxRowsInMemory = maxRowsInMemory;
      return this;
    }

    public Builder appendableIndexSpec(AppendableIndexSpec appendableIndexSpec)
    {
      this.appendableIndexSpec = appendableIndexSpec;
      return this;
    }

    public Builder maxBytesInMemory(Long maxBytesInMemory)
    {
      this.maxBytesInMemory = maxBytesInMemory;
      return this;
    }

    public Builder maxTotalRows(Long maxTotalRows)
    {
      this.maxTotalRows = maxTotalRows;
      return this;
    }

    public Builder splitHintSpec(SplitHintSpec splitHintSpec)
    {
      this.splitHintSpec = splitHintSpec;
      return this;
    }

    public Builder partitionsSpec(PartitionsSpec partitionsSpec)
    {
      this.partitionsSpec = partitionsSpec;
      return this;
    }

    public Builder indexSpec(IndexSpec indexSpec)
    {
      this.indexSpec = indexSpec;
      return this;
    }

    public Builder indexSpecForIntermediatePersists(IndexSpec indexSpecForIntermediatePersists)
    {
      this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists;
      return this;
    }

    public Builder maxPendingPersists(Integer maxPendingPersists)
    {
      this.maxPendingPersists = maxPendingPersists;
      return this;
    }

    public Builder pushTimeout(Long pushTimeout)
    {
      this.pushTimeout = pushTimeout;
      return this;
    }

    public Builder segmentWriteOutMediumFactory(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
    {
      this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
      return this;
    }

    public Builder maxNumConcurrentSubTasks(Integer maxNumConcurrentSubTasks)
    {
      this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
      return this;
    }

    public Builder maxRetry(Integer maxRetry)
    {
      this.maxRetry = maxRetry;
      return this;
    }

    public Builder taskStatusCheckPeriodMs(Long taskStatusCheckPeriodMs)
    {
      this.taskStatusCheckPeriodMs = taskStatusCheckPeriodMs;
      return this;
    }

    public Builder chatHandlerTimeout(Duration chatHandlerTimeout)
    {
      this.chatHandlerTimeout = chatHandlerTimeout;
      return this;
    }

    public Builder chatHandlerNumRetries(Integer chatHandlerNumRetries)
    {
      this.chatHandlerNumRetries = chatHandlerNumRetries;
      return this;
    }

    public Builder maxNumSegmentsToMerge(Integer maxNumSegmentsToMerge)
    {
      this.maxNumSegmentsToMerge = maxNumSegmentsToMerge;
      return this;
    }

    public Builder totalNumMergeTasks(Integer totalNumMergeTasks)
    {
      this.totalNumMergeTasks = totalNumMergeTasks;
      return this;
    }

    public Builder maxColumnsToMerge(Integer maxColumnsToMerge)
    {
      this.maxColumnsToMerge = maxColumnsToMerge;
      return this;
    }

    public UserCompactionTaskQueryTuningConfig build()
    {
      return new UserCompactionTaskQueryTuningConfig(
          maxRowsInMemory,
          appendableIndexSpec,
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
          maxColumnsToMerge
      );
    }
  }
}
