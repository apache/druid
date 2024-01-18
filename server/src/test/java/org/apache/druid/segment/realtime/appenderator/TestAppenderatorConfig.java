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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class TestAppenderatorConfig implements AppenderatorConfig
{
  private final AppendableIndexSpec appendableIndexSpec;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final boolean skipBytesInMemoryOverheadCheck;
  private final int maxColumnsToMerge;
  private final PartitionsSpec partitionsSpec;
  private final IndexSpec indexSpec;
  private final File basePersistDirectory;
  private final int maxPendingPersists;
  private final boolean reportParseExceptions;
  private final long pushTimeout;
  private final IndexSpec indexSpecForIntermediatePersists;
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  public TestAppenderatorConfig(
      AppendableIndexSpec appendableIndexSpec,
      Integer maxRowsInMemory,
      Long maxBytesInMemory,
      Boolean skipBytesInMemoryOverheadCheck,
      IndexSpec indexSpec,
      Integer maxPendingPersists,
      Boolean reportParseExceptions,
      Long pushTimeout,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      Integer maxColumnsToMerge,
      File basePersistDirectory
  )
  {
    this.appendableIndexSpec = appendableIndexSpec;
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxBytesInMemory = maxBytesInMemory;
    this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck;
    this.indexSpec = indexSpec;
    this.maxPendingPersists = maxPendingPersists;
    this.reportParseExceptions = reportParseExceptions;
    this.pushTimeout = pushTimeout;
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.maxColumnsToMerge = maxColumnsToMerge;
    this.basePersistDirectory = basePersistDirectory;

    this.partitionsSpec = null;
    this.indexSpecForIntermediatePersists = this.indexSpec;
  }

  @Override
  public TestAppenderatorConfig withBasePersistDirectory(File dir)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public AppendableIndexSpec getAppendableIndexSpec()
  {
    return appendableIndexSpec;
  }

  @Override
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @Override
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  @Override
  public boolean isSkipBytesInMemoryOverheadCheck()
  {
    return skipBytesInMemoryOverheadCheck;
  }

  @Nullable
  @Override
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @Override
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @Override
  public IndexSpec getIndexSpecForIntermediatePersists()
  {
    return indexSpecForIntermediatePersists;
  }

  @Override
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @Override
  public boolean isReportParseExceptions()
  {
    return reportParseExceptions;
  }

  @Nullable
  @Override
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
  }

  @Override
  public int getMaxColumnsToMerge()
  {
    return maxColumnsToMerge;
  }

  @Override
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @Override
  public Period getIntermediatePersistPeriod()
  {
    return new Period(Integer.MAX_VALUE); // intermediate persist doesn't make much sense for batch jobs
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
    TestAppenderatorConfig that = (TestAppenderatorConfig) o;
    return Objects.equals(appendableIndexSpec, that.appendableIndexSpec) &&
           maxRowsInMemory == that.maxRowsInMemory &&
           maxBytesInMemory == that.maxBytesInMemory &&
           skipBytesInMemoryOverheadCheck == that.skipBytesInMemoryOverheadCheck &&
           maxColumnsToMerge == that.maxColumnsToMerge &&
           maxPendingPersists == that.maxPendingPersists &&
           reportParseExceptions == that.reportParseExceptions &&
           pushTimeout == that.pushTimeout &&
           Objects.equals(partitionsSpec, that.partitionsSpec) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(indexSpecForIntermediatePersists, that.indexSpecForIntermediatePersists) &&
           Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
           Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        maxColumnsToMerge,
        partitionsSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        basePersistDirectory,
        maxPendingPersists,
        reportParseExceptions,
        pushTimeout,
        segmentWriteOutMediumFactory
    );
  }

  @Override
  public String toString()
  {
    return "TestAppenderatorConfig{" +
           "maxRowsInMemory=" + maxRowsInMemory +
           ", maxBytesInMemory=" + maxBytesInMemory +
           ", skipBytesInMemoryOverheadCheck=" + skipBytesInMemoryOverheadCheck +
           ", maxColumnsToMerge=" + maxColumnsToMerge +
           ", partitionsSpec=" + partitionsSpec +
           ", indexSpec=" + indexSpec +
           ", indexSpecForIntermediatePersists=" + indexSpecForIntermediatePersists +
           ", basePersistDirectory=" + basePersistDirectory +
           ", maxPendingPersists=" + maxPendingPersists +
           ", reportParseExceptions=" + reportParseExceptions +
           ", pushTimeout=" + pushTimeout +
           ", segmentWriteOutMediumFactory=" + segmentWriteOutMediumFactory +
           '}';
  }
}
