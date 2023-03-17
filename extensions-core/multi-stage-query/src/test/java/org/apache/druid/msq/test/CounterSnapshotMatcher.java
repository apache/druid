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

package org.apache.druid.msq.test;

import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.QueryCounterSnapshot;
import org.apache.druid.msq.counters.SegmentGenerationProgressCounter;
import org.junit.Assert;

/**
 * Utility class to build instances of {@link QueryCounterSnapshot} used in tests.
 */
public class CounterSnapshotMatcher
{
  private long[] rows;
  private long[] bytes;
  private long[] frames;
  private long[] files;
  private long[] totalFiles;
  private Long segmentRowsProcessed;

  public static CounterSnapshotMatcher with()
  {
    return new CounterSnapshotMatcher();
  }

  public CounterSnapshotMatcher rows(long... rows)
  {
    this.rows = rows;
    return this;
  }
  public CounterSnapshotMatcher segmentRowsProcessed(long segmentRowsProcessed)
  {
    this.segmentRowsProcessed = segmentRowsProcessed;
    return this;
  }

  public CounterSnapshotMatcher bytes(long... bytes)
  {
    this.bytes = bytes;
    return this;
  }

  public CounterSnapshotMatcher frames(long... frames)
  {
    this.frames = frames;
    return this;
  }

  public CounterSnapshotMatcher files(long... files)
  {
    this.files = files;
    return this;
  }

  public CounterSnapshotMatcher totalFiles(long... totalFiles)
  {
    this.totalFiles = totalFiles;
    return this;
  }

  /**
   * Asserts that the matcher matches the queryCounterSnapshot parameter. If a parameter in this class is null, the
   * match is not checked
   */
  public void matchQuerySnapshot(String errorMessageFormat, QueryCounterSnapshot queryCounterSnapshot)
  {
    if (rows != null) {
      Assert.assertArrayEquals(errorMessageFormat, rows, ((ChannelCounters.Snapshot) queryCounterSnapshot).getRows());
    }
    if (bytes != null) {
      Assert.assertArrayEquals(errorMessageFormat, bytes, ((ChannelCounters.Snapshot) queryCounterSnapshot).getBytes());
    }
    if (frames != null) {
      Assert.assertArrayEquals(errorMessageFormat, frames, ((ChannelCounters.Snapshot) queryCounterSnapshot).getFrames());
    }
    if (files != null) {
      Assert.assertArrayEquals(errorMessageFormat, files, ((ChannelCounters.Snapshot) queryCounterSnapshot).getFiles());
    }
    if (totalFiles != null) {
      Assert.assertArrayEquals(errorMessageFormat, totalFiles, ((ChannelCounters.Snapshot) queryCounterSnapshot).getTotalFiles());
    }
    if (segmentRowsProcessed != null) {
      Assert.assertEquals(errorMessageFormat, segmentRowsProcessed.longValue(), ((SegmentGenerationProgressCounter.Snapshot) queryCounterSnapshot).getRowsProcessed());
    }
  }
}
