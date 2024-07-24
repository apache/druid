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

package org.apache.druid.indexing.common.task;

import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;

/**
 * Builder utility for various task tuning configs.
 *
 * @param <C> Type of config that is being built
 * @see TuningConfigBuilder#forIndexTask()
 * @see TuningConfigBuilder#forParallelIndexTask()
 * @see TuningConfigBuilder#forCompactionTask()
 */
public abstract class TuningConfigBuilder<C>
{
  protected Integer targetPartitionSize;
  protected Integer maxRowsPerSegment;
  protected AppendableIndexSpec appendableIndexSpec;
  protected Integer maxRowsInMemory;
  protected Long maxBytesInMemory;
  protected Boolean skipBytesInMemoryOverheadCheck;
  protected Long maxTotalRows;
  protected Integer numShards;
  protected SplitHintSpec splitHintSpec;
  protected PartitionsSpec partitionsSpec;
  protected IndexSpec indexSpec;
  protected IndexSpec indexSpecForIntermediatePersists;
  protected Integer maxPendingPersists;
  protected Boolean forceGuaranteedRollup;
  protected Boolean reportParseExceptions;
  protected Long publishTimeout;
  protected Long pushTimeout;
  protected SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
  protected Integer maxNumSubTasks;
  protected Integer maxNumConcurrentSubTasks;
  protected Integer maxRetry;
  protected Long taskStatusCheckPeriodMs;
  protected Duration chatHandlerTimeout;
  protected Integer chatHandlerNumRetries;
  protected Integer maxNumSegmentsToMerge;
  protected Integer totalNumMergeTasks;
  protected Boolean logParseExceptions;
  protected Integer maxParseExceptions;
  protected Integer maxSavedParseExceptions;
  protected Integer maxColumnsToMerge;
  protected Long awaitSegmentAvailabilityTimeoutMillis;
  protected Integer maxAllowedLockCount;
  protected Integer numPersistThreads;

  public TuningConfigBuilder<C> withTargetPartitionSize(Integer targetPartitionSize)
  {
    this.targetPartitionSize = targetPartitionSize;
    return this;
  }

  public TuningConfigBuilder<C> withMaxRowsPerSegment(Integer maxRowsPerSegment)
  {
    this.maxRowsPerSegment = maxRowsPerSegment;
    return this;
  }

  public TuningConfigBuilder<C> withAppendableIndexSpec(AppendableIndexSpec appendableIndexSpec)
  {
    this.appendableIndexSpec = appendableIndexSpec;
    return this;
  }

  public TuningConfigBuilder<C> withMaxRowsInMemory(Integer maxRowsInMemory)
  {
    this.maxRowsInMemory = maxRowsInMemory;
    return this;
  }

  public TuningConfigBuilder<C> withMaxBytesInMemory(Long maxBytesInMemory)
  {
    this.maxBytesInMemory = maxBytesInMemory;
    return this;
  }

  public TuningConfigBuilder<C> withSkipBytesInMemoryOverheadCheck(Boolean skipBytesInMemoryOverheadCheck)
  {
    this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck;
    return this;
  }

  public TuningConfigBuilder<C> withMaxTotalRows(Long maxTotalRows)
  {
    this.maxTotalRows = maxTotalRows;
    return this;
  }

  public TuningConfigBuilder<C> withNumShards(Integer numShards)
  {
    this.numShards = numShards;
    return this;
  }

  public TuningConfigBuilder<C> withSplitHintSpec(SplitHintSpec splitHintSpec)
  {
    this.splitHintSpec = splitHintSpec;
    return this;
  }

  public TuningConfigBuilder<C> withPartitionsSpec(PartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
    return this;
  }

  public TuningConfigBuilder<C> withIndexSpec(IndexSpec indexSpec)
  {
    this.indexSpec = indexSpec;
    return this;
  }

  public TuningConfigBuilder<C> withIndexSpecForIntermediatePersists(IndexSpec indexSpecForIntermediatePersists)
  {
    this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists;
    return this;
  }

  public TuningConfigBuilder<C> withMaxPendingPersists(Integer maxPendingPersists)
  {
    this.maxPendingPersists = maxPendingPersists;
    return this;
  }

  public TuningConfigBuilder<C> withForceGuaranteedRollup(Boolean forceGuaranteedRollup)
  {
    this.forceGuaranteedRollup = forceGuaranteedRollup;
    return this;
  }

  public TuningConfigBuilder<C> withReportParseExceptions(Boolean reportParseExceptions)
  {
    this.reportParseExceptions = reportParseExceptions;
    return this;
  }

  public TuningConfigBuilder<C> withPushTimeout(Long pushTimeout)
  {
    this.pushTimeout = pushTimeout;
    return this;
  }

  public TuningConfigBuilder<C> withPublishTimeout(Long publishTimeout)
  {
    this.publishTimeout = publishTimeout;
    return this;
  }

  public TuningConfigBuilder<C> withSegmentWriteOutMediumFactory(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    return this;
  }

  public TuningConfigBuilder<C> withMaxNumSubTasks(Integer maxNumSubTasks)
  {
    this.maxNumSubTasks = maxNumSubTasks;
    return this;
  }

  public TuningConfigBuilder<C> withMaxNumConcurrentSubTasks(Integer maxNumConcurrentSubTasks)
  {
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
    return this;
  }

  public TuningConfigBuilder<C> withMaxRetry(Integer maxRetry)
  {
    this.maxRetry = maxRetry;
    return this;
  }

  public TuningConfigBuilder<C> withTaskStatusCheckPeriodMs(Long taskStatusCheckPeriodMs)
  {
    this.taskStatusCheckPeriodMs = taskStatusCheckPeriodMs;
    return this;
  }

  public TuningConfigBuilder<C> withChatHandlerTimeout(Duration chatHandlerTimeout)
  {
    this.chatHandlerTimeout = chatHandlerTimeout;
    return this;
  }

  public TuningConfigBuilder<C> withChatHandlerNumRetries(Integer chatHandlerNumRetries)
  {
    this.chatHandlerNumRetries = chatHandlerNumRetries;
    return this;
  }

  public TuningConfigBuilder<C> withMaxNumSegmentsToMerge(Integer maxNumSegmentsToMerge)
  {
    this.maxNumSegmentsToMerge = maxNumSegmentsToMerge;
    return this;
  }

  public TuningConfigBuilder<C> withTotalNumMergeTasks(Integer totalNumMergeTasks)
  {
    this.totalNumMergeTasks = totalNumMergeTasks;
    return this;
  }

  public TuningConfigBuilder<C> withLogParseExceptions(Boolean logParseExceptions)
  {
    this.logParseExceptions = logParseExceptions;
    return this;
  }

  public TuningConfigBuilder<C> withMaxParseExceptions(Integer maxParseExceptions)
  {
    this.maxParseExceptions = maxParseExceptions;
    return this;
  }

  public TuningConfigBuilder<C> withMaxSavedParseExceptions(Integer maxSavedParseExceptions)
  {
    this.maxSavedParseExceptions = maxSavedParseExceptions;
    return this;
  }

  public TuningConfigBuilder<C> withMaxColumnsToMerge(Integer maxColumnsToMerge)
  {
    this.maxColumnsToMerge = maxColumnsToMerge;
    return this;
  }

  public TuningConfigBuilder<C> withAwaitSegmentAvailabilityTimeoutMillis(Long awaitSegmentAvailabilityTimeoutMillis)
  {
    this.awaitSegmentAvailabilityTimeoutMillis = awaitSegmentAvailabilityTimeoutMillis;
    return this;
  }

  public TuningConfigBuilder<C> withNumPersistThreads(Integer numPersistThreads)
  {
    this.numPersistThreads = numPersistThreads;
    return this;
  }

  public TuningConfigBuilder<C> withMaxAllowedLockCount(Integer maxAllowedLockCount)
  {
    this.maxAllowedLockCount = maxAllowedLockCount;
    return this;
  }

  public abstract C build();

  /**
   * Creates a new builder for {@link CompactionTask.CompactionTuningConfig}.
   */
  public static Compact forCompactionTask()
  {
    return new Compact();
  }

  /**
   * Creates a new builder for {@link ParallelIndexTuningConfig}.
   */
  public static ParallelIndex forParallelIndexTask()
  {
    return new ParallelIndex();
  }

  /**
   * Creates a new builder for {@link IndexTask.IndexTuningConfig}.
   */
  public static Index forIndexTask()
  {
    return new Index();
  }

  public static class Index extends TuningConfigBuilder<IndexTask.IndexTuningConfig>
  {
    @Override
    public IndexTask.IndexTuningConfig build()
    {
      return new IndexTask.IndexTuningConfig(
          targetPartitionSize,
          maxRowsPerSegment,
          appendableIndexSpec,
          maxRowsInMemory,
          maxBytesInMemory,
          skipBytesInMemoryOverheadCheck,
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
          publishTimeout,
          pushTimeout,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          maxColumnsToMerge,
          awaitSegmentAvailabilityTimeoutMillis,
          numPersistThreads
      );
    }
  }

  public static class ParallelIndex extends TuningConfigBuilder<ParallelIndexTuningConfig>
  {
    @Override
    public ParallelIndexTuningConfig build()
    {
      return new ParallelIndexTuningConfig(
          targetPartitionSize,
          maxRowsPerSegment,
          appendableIndexSpec,
          maxRowsInMemory,
          maxBytesInMemory,
          skipBytesInMemoryOverheadCheck,
          maxTotalRows,
          numShards,
          splitHintSpec,
          partitionsSpec,
          indexSpec,
          indexSpecForIntermediatePersists,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          segmentWriteOutMediumFactory,
          maxNumSubTasks,
          maxNumConcurrentSubTasks,
          maxRetry,
          taskStatusCheckPeriodMs,
          chatHandlerTimeout,
          chatHandlerNumRetries,
          maxNumSegmentsToMerge,
          totalNumMergeTasks,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          maxColumnsToMerge,
          awaitSegmentAvailabilityTimeoutMillis,
          maxAllowedLockCount,
          numPersistThreads
      );
    }
  }

  public static class Compact extends TuningConfigBuilder<CompactionTask.CompactionTuningConfig>
  {
    @Override
    public CompactionTask.CompactionTuningConfig build()
    {
      return new CompactionTask.CompactionTuningConfig(
          targetPartitionSize,
          maxRowsPerSegment,
          appendableIndexSpec,
          maxRowsInMemory,
          maxBytesInMemory,
          skipBytesInMemoryOverheadCheck,
          maxTotalRows,
          numShards,
          splitHintSpec,
          partitionsSpec,
          indexSpec,
          indexSpecForIntermediatePersists,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          segmentWriteOutMediumFactory,
          maxNumSubTasks,
          maxNumConcurrentSubTasks,
          maxRetry,
          taskStatusCheckPeriodMs,
          chatHandlerTimeout,
          chatHandlerNumRetries,
          maxNumSegmentsToMerge,
          totalNumMergeTasks,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          maxColumnsToMerge,
          awaitSegmentAvailabilityTimeoutMillis,
          numPersistThreads
      );
    }
  }
}
