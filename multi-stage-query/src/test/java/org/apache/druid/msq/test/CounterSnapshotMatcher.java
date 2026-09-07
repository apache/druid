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
import org.junit.jupiter.api.Assertions;

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
  private long[] loadBytes;
  private long[] loadTime;
  private long[] loadWait;
  private long[] loadFiles;
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

  public CounterSnapshotMatcher loadBytes(long... loadBytes)
  {
    this.loadBytes = loadBytes;
    return this;
  }

  public CounterSnapshotMatcher loadTime(long... loadTime)
  {
    this.loadTime = loadTime;
    return this;
  }

  public CounterSnapshotMatcher loadWait(long... loadWait)
  {
    this.loadWait = loadWait;
    return this;
  }

  public CounterSnapshotMatcher loadFiles(long... loadFiles)
  {
    this.loadFiles = loadFiles;
    return this;
  }

  /**
   * Asserts that the matcher matches the queryCounterSnapshot parameter. If a parameter in this class is null, the
   * match is not checked
   */
  public void matchQuerySnapshot(String errorMessageFormat, QueryCounterSnapshot queryCounterSnapshot)
  {
    if (rows != null) {
      Assertions.assertArrayEquals(rows, ((ChannelCounters.Snapshot) queryCounterSnapshot).getRows(), errorMessageFormat);
    }
    if (bytes != null) {
      Assertions.assertArrayEquals(bytes, ((ChannelCounters.Snapshot) queryCounterSnapshot).getBytes(), errorMessageFormat);
    }
    if (frames != null) {
      Assertions.assertArrayEquals(frames, ((ChannelCounters.Snapshot) queryCounterSnapshot).getFrames(), errorMessageFormat);
    }
    if (files != null) {
      Assertions.assertArrayEquals(files, ((ChannelCounters.Snapshot) queryCounterSnapshot).getFiles(), errorMessageFormat);
    }
    if (totalFiles != null) {
      Assertions.assertArrayEquals(totalFiles, ((ChannelCounters.Snapshot) queryCounterSnapshot).getTotalFiles(), errorMessageFormat);
    }
    if (loadBytes != null) {
      Assertions.assertArrayEquals(loadBytes, ((ChannelCounters.Snapshot) queryCounterSnapshot).getLoadBytes(), errorMessageFormat);
    }
    if (loadTime != null) {
      Assertions.assertArrayEquals(loadTime, ((ChannelCounters.Snapshot) queryCounterSnapshot).getLoadTime(), errorMessageFormat);
    }
    if (loadWait != null) {
      Assertions.assertArrayEquals(loadWait, ((ChannelCounters.Snapshot) queryCounterSnapshot).getLoadWait(), errorMessageFormat);
    }
    if (loadFiles != null) {
      Assertions.assertArrayEquals(loadFiles, ((ChannelCounters.Snapshot) queryCounterSnapshot).getLoadFiles(), errorMessageFormat);
    }
    if (segmentRowsProcessed != null) {
      Assertions.assertEquals(segmentRowsProcessed.longValue(), ((SegmentGenerationProgressCounter.Snapshot) queryCounterSnapshot).getRowsProcessed(), errorMessageFormat);
    }
  }
}
