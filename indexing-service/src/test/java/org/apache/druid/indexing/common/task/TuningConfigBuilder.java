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

import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;

/**
 * Builder utility for various task tuning configs.
 *
 * @param <C> Type of config that is being built
 * @see TuningConfigBuilder#forIndexTask()
 * @see TuningConfigBuilder#forParallelIndexTask()
 * @see TuningConfigBuilder#forCompactionTask()
 */
public abstract class TuningConfigBuilder<C> extends org.apache.druid.segment.indexing.TuningConfigBuilder<C>
{
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
