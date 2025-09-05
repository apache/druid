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
 * @param <Self> Type of this builder itself
 * @see TuningConfigBuilder#forIndexTask()
 * @see TuningConfigBuilder#forParallelIndexTask()
 * @see TuningConfigBuilder#forCompactionTask()
 */
public abstract class TuningConfigBuilder<Self extends TuningConfigBuilder<Self, C>, C>
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

  public Self withTargetPartitionSize(Integer targetPartitionSize)
  {
    this.targetPartitionSize = targetPartitionSize;
    return self();
  }

  public Self withMaxRowsPerSegment(Integer maxRowsPerSegment)
  {
    this.maxRowsPerSegment = maxRowsPerSegment;
    return self();
  }

  public Self withAppendableIndexSpec(AppendableIndexSpec appendableIndexSpec)
  {
    this.appendableIndexSpec = appendableIndexSpec;
    return self();
  }

  public Self withMaxRowsInMemory(Integer maxRowsInMemory)
  {
    this.maxRowsInMemory = maxRowsInMemory;
    return self();
  }

  public Self withMaxBytesInMemory(Long maxBytesInMemory)
  {
    this.maxBytesInMemory = maxBytesInMemory;
    return self();
  }

  public Self withSkipBytesInMemoryOverheadCheck(Boolean skipBytesInMemoryOverheadCheck)
  {
    this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck;
    return self();
  }

  public Self withMaxTotalRows(Long maxTotalRows)
  {
    this.maxTotalRows = maxTotalRows;
    return self();
  }

  public Self withNumShards(Integer numShards)
  {
    this.numShards = numShards;
    return self();
  }

  public Self withSplitHintSpec(SplitHintSpec splitHintSpec)
  {
    this.splitHintSpec = splitHintSpec;
    return self();
  }

  public Self withPartitionsSpec(PartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
    return self();
  }

  public Self withIndexSpec(IndexSpec indexSpec)
  {
    this.indexSpec = indexSpec;
    return self();
  }

  public Self withIndexSpecForIntermediatePersists(IndexSpec indexSpecForIntermediatePersists)
  {
    this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists;
    return self();
  }

  public Self withMaxPendingPersists(Integer maxPendingPersists)
  {
    this.maxPendingPersists = maxPendingPersists;
    return self();
  }

  public Self withForceGuaranteedRollup(Boolean forceGuaranteedRollup)
  {
    this.forceGuaranteedRollup = forceGuaranteedRollup;
    return self();
  }

  public Self withReportParseExceptions(Boolean reportParseExceptions)
  {
    this.reportParseExceptions = reportParseExceptions;
    return self();
  }

  public Self withPushTimeout(Long pushTimeout)
  {
    this.pushTimeout = pushTimeout;
    return self();
  }

  public Self withPublishTimeout(Long publishTimeout)
  {
    this.publishTimeout = publishTimeout;
    return self();
  }

  public Self withSegmentWriteOutMediumFactory(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    return self();
  }

  public Self withMaxNumSubTasks(Integer maxNumSubTasks)
  {
    this.maxNumSubTasks = maxNumSubTasks;
    return self();
  }

  public Self withMaxNumConcurrentSubTasks(Integer maxNumConcurrentSubTasks)
  {
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
    return self();
  }

  public Self withMaxRetry(Integer maxRetry)
  {
    this.maxRetry = maxRetry;
    return self();
  }

  public Self withTaskStatusCheckPeriodMs(Long taskStatusCheckPeriodMs)
  {
    this.taskStatusCheckPeriodMs = taskStatusCheckPeriodMs;
    return self();
  }

  public Self withChatHandlerTimeout(Duration chatHandlerTimeout)
  {
    this.chatHandlerTimeout = chatHandlerTimeout;
    return self();
  }

  public Self withChatHandlerNumRetries(Integer chatHandlerNumRetries)
  {
    this.chatHandlerNumRetries = chatHandlerNumRetries;
    return self();
  }

  public Self withMaxNumSegmentsToMerge(Integer maxNumSegmentsToMerge)
  {
    this.maxNumSegmentsToMerge = maxNumSegmentsToMerge;
    return self();
  }

  public Self withTotalNumMergeTasks(Integer totalNumMergeTasks)
  {
    this.totalNumMergeTasks = totalNumMergeTasks;
    return self();
  }

  public Self withLogParseExceptions(Boolean logParseExceptions)
  {
    this.logParseExceptions = logParseExceptions;
    return self();
  }

  public Self withMaxParseExceptions(Integer maxParseExceptions)
  {
    this.maxParseExceptions = maxParseExceptions;
    return self();
  }

  public Self withMaxSavedParseExceptions(Integer maxSavedParseExceptions)
  {
    this.maxSavedParseExceptions = maxSavedParseExceptions;
    return self();
  }

  public Self withMaxColumnsToMerge(Integer maxColumnsToMerge)
  {
    this.maxColumnsToMerge = maxColumnsToMerge;
    return self();
  }

  public Self withAwaitSegmentAvailabilityTimeoutMillis(Long awaitSegmentAvailabilityTimeoutMillis)
  {
    this.awaitSegmentAvailabilityTimeoutMillis = awaitSegmentAvailabilityTimeoutMillis;
    return self();
  }

  public Self withNumPersistThreads(Integer numPersistThreads)
  {
    this.numPersistThreads = numPersistThreads;
    return self();
  }

  public Self withMaxAllowedLockCount(Integer maxAllowedLockCount)
  {
    this.maxAllowedLockCount = maxAllowedLockCount;
    return self();
  }
  
  @SuppressWarnings("unchecked")
  private Self self()
  {
    return (Self) this;
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

  public static class Index extends TuningConfigBuilder<Index, IndexTask.IndexTuningConfig>
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

  public static class ParallelIndex extends TuningConfigBuilder<ParallelIndex, ParallelIndexTuningConfig>
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

  public static class Compact extends TuningConfigBuilder<Compact, CompactionTask.CompactionTuningConfig>
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
