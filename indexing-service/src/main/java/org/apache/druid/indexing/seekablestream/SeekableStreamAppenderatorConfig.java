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

package org.apache.druid.indexing.seekablestream;

import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.utils.JvmUtils;
import org.apache.druid.utils.RuntimeInfo;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class SeekableStreamAppenderatorConfig implements AppenderatorConfig
{
  private static final Logger log = new Logger(SeekableStreamAppenderatorConfig.class);

  /**
   * Rough estimate of the on-heap overhead of reading a column. Determined empirically through heap dumps.
   */
  public static final int APPENDERATOR_MERGE_ROUGH_HEAP_MEMORY_PER_COLUMN = 3_000;

  /**
   * Rough estimate of the off-heap overhead of reading a column. This is 1.5x the size of a decompression buffer.
   * The 1.5x allows for some headroom for unaccounted-for memory usage. The decompression buffer size is about right
   * when a compressing {@link IndexSpec} is in play. It is an overestimate when compression is disabled for
   * intermediate persists, but this isn't currently the default behavior, so we're wanting to be conservative here.
   *
   * See also {@link AppenderatorConfig#getIndexSpecForIntermediatePersists()} for how compression can be disabled
   * for intermediate persists.
   */
  public static final int APPENDERATOR_MERGE_ROUGH_DIRECT_MEMORY_PER_COLUMN = (int) ((1 << 16) * 1.5);

  private final SeekableStreamIndexTaskTuningConfig tuningConfig;
  private final int maxColumnsToMerge;

  private SeekableStreamAppenderatorConfig(SeekableStreamIndexTaskTuningConfig tuningConfig, int maxColumnsToMerge)
  {
    this.tuningConfig = tuningConfig;
    this.maxColumnsToMerge = maxColumnsToMerge;
  }

  public static SeekableStreamAppenderatorConfig fromTuningConfig(
      final SeekableStreamIndexTaskTuningConfig tuningConfig,
      @Nullable final DruidProcessingConfig processingConfig
  )
  {
    if (processingConfig == null) {
      return new SeekableStreamAppenderatorConfig(tuningConfig, tuningConfig.getMaxColumnsToMerge());
    } else {
      final int maxColumnsToMerge;
      if (tuningConfig.getMaxColumnsToMerge() == SeekableStreamIndexTaskTuningConfig.DEFAULT_MAX_COLUMNS_TO_MERGE) {
        maxColumnsToMerge = calculateDefaultMaxColumnsToMerge(
            JvmUtils.getRuntimeInfo(),
            processingConfig,
            tuningConfig
        );
      } else {
        maxColumnsToMerge = tuningConfig.getMaxColumnsToMerge();
      }

      return new SeekableStreamAppenderatorConfig(tuningConfig, maxColumnsToMerge);
    }
  }

  @Override
  public boolean isReportParseExceptions()
  {
    return tuningConfig.isReportParseExceptions();
  }

  @Override
  public int getMaxPendingPersists()
  {
    return tuningConfig.getMaxPendingPersists();
  }

  @Override
  public boolean isSkipBytesInMemoryOverheadCheck()
  {
    return tuningConfig.isSkipBytesInMemoryOverheadCheck();
  }

  @Override
  public Period getIntermediatePersistPeriod()
  {
    return tuningConfig.getIntermediatePersistPeriod();
  }

  @Override
  public File getBasePersistDirectory()
  {
    return tuningConfig.getBasePersistDirectory();
  }

  @Override
  public AppenderatorConfig withBasePersistDirectory(File basePersistDirectory)
  {
    return new SeekableStreamAppenderatorConfig(
        tuningConfig.withBasePersistDirectory(basePersistDirectory),
        maxColumnsToMerge
    );
  }

  @Override
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return tuningConfig.getSegmentWriteOutMediumFactory();
  }

  @Override
  public AppendableIndexSpec getAppendableIndexSpec()
  {
    return tuningConfig.getAppendableIndexSpec();
  }

  @Override
  public int getMaxRowsInMemory()
  {
    return tuningConfig.getMaxRowsInMemory();
  }

  @Override
  public long getMaxBytesInMemory()
  {
    return tuningConfig.getMaxBytesInMemory();
  }

  @Override
  public PartitionsSpec getPartitionsSpec()
  {
    return tuningConfig.getPartitionsSpec();
  }

  @Override
  public IndexSpec getIndexSpec()
  {
    return tuningConfig.getIndexSpec();
  }

  @Override
  public IndexSpec getIndexSpecForIntermediatePersists()
  {
    return tuningConfig.getIndexSpecForIntermediatePersists();
  }

  @Override
  public int getNumPersistThreads()
  {
    return tuningConfig.getNumPersistThreads();
  }

  @Override
  public Integer getMaxRowsPerSegment()
  {
    return tuningConfig.getMaxRowsPerSegment();
  }

  @Override
  public Long getMaxTotalRows()
  {
    return tuningConfig.getMaxTotalRows();
  }

  @Override
  public int getMaxColumnsToMerge()
  {
    return maxColumnsToMerge;
  }

  @Override
  public boolean isReleaseLocksOnHandoff()
  {
    return tuningConfig.isReleaseLocksOnHandoff();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SeekableStreamAppenderatorConfig that = (SeekableStreamAppenderatorConfig) o;
    return maxColumnsToMerge == that.maxColumnsToMerge && Objects.equals(tuningConfig, that.tuningConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tuningConfig, maxColumnsToMerge);
  }

  /**
   * Calculates the value to use for "maxColumnsToMerge" when it has been set to
   * {@link SeekableStreamIndexTaskTuningConfig#DEFAULT_MAX_COLUMNS_TO_MERGE}.
   */
  static int calculateDefaultMaxColumnsToMerge(
      final RuntimeInfo runtimeInfo,
      final DruidProcessingConfig processingConfig,
      final TuningConfig tuningConfig
  )
  {
    final int maxColumnsToMergeBasedOnHeapMemory = getMaxColumnsToMergeBasedOnHeapMemory(tuningConfig);
    final int maxColumnsToMergeBasedOnDirectMemory =
        getMaxColumnsToMergeBasedOnDirectMemory(runtimeInfo, processingConfig);

    return Math.min(maxColumnsToMergeBasedOnHeapMemory, maxColumnsToMergeBasedOnDirectMemory);
  }

  /**
   * Calculates the value to use for "maxColumnsToMerge" based purely on heap memory.
   */
  static int getMaxColumnsToMergeBasedOnHeapMemory(TuningConfig tuningConfig)
  {
    final long heapMemoryForMerging;

    if (tuningConfig.getMaxBytesInMemory() >= 0) {
      // maxBytesInMemory is active and represents the amount of heap memory used for indexing.
      // Use an equal amount of heap memory for merging.
      heapMemoryForMerging = tuningConfig.getMaxBytesInMemoryOrDefault();
    } else {
      // maxBytesInMemory was set to unlimited by the user; use what that _would_ be if it was set to automatic.
      heapMemoryForMerging = tuningConfig.getAppendableIndexSpec().getDefaultMaxBytesInMemory();
    }

    return (int) Math.min(heapMemoryForMerging / APPENDERATOR_MERGE_ROUGH_HEAP_MEMORY_PER_COLUMN, Integer.MAX_VALUE);
  }

  /**
   * Calculates the value to use for "maxColumnsToMerge" based purely on direct memory.
   */
  static int getMaxColumnsToMergeBasedOnDirectMemory(
      final RuntimeInfo runtimeInfo,
      final DruidProcessingConfig processingConfig
  )
  {
    // Queries requires one processing buffer per thread, plus all the merge buffers.
    final long directMemoryForQueries =
        (long) processingConfig.intermediateComputeSizeBytes() *
        (processingConfig.getNumThreads() + processingConfig.getNumMergeBuffers());

    long directMemorySizeBytes;

    try {
      directMemorySizeBytes = runtimeInfo.getDirectMemorySizeBytes();
    }
    catch (UnsupportedOperationException e) {
      // Cannot find direct memory, assume it was configured according to our guidelines (at least big enough to
      // hold one extra processing buffer).
      directMemorySizeBytes = directMemoryForQueries + processingConfig.intermediateComputeSizeBytes();

      log.noStackTrace().warn(
          e,
          "Ignoring direct memory when sizing maxColumnsToMerge; cannot retrieve. "
          + "Assuming direct memory is at least [%,d] bytes.",
          directMemorySizeBytes
      );
    }

    return (int) Math.min(
        (directMemorySizeBytes - directMemoryForQueries) / APPENDERATOR_MERGE_ROUGH_DIRECT_MEMORY_PER_COLUMN,
        Integer.MAX_VALUE
    );
  }
}
