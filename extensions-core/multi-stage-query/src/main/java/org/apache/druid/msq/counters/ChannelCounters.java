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

package org.apache.druid.msq.counters;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.druid.frame.Frame;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * Counters for inputs and outputs. Created by {@link CounterTracker#channel}.
 */
public class ChannelCounters implements QueryCounter
{
  private static final int NO_PARTITION = 0;

  @GuardedBy("this")
  private final LongList rows = new LongArrayList();

  @GuardedBy("this")
  private final LongList bytes = new LongArrayList();

  @GuardedBy("this")
  private final LongList frames = new LongArrayList();

  @GuardedBy("this")
  private final LongList files = new LongArrayList();

  @GuardedBy("this")
  private final LongList totalFiles = new LongArrayList();

  public void incrementRowCount()
  {
    add(NO_PARTITION, 1, 0, 0, 0);
  }

  public void incrementFileCount()
  {
    add(NO_PARTITION, 0, 0, 0, 1);
  }

  public void addFile(final long nRows, final long nBytes)
  {
    add(NO_PARTITION, nRows, nBytes, 0, 1);
  }

  public void addFrame(final int partitionNumber, final Frame frame)
  {
    add(partitionNumber, frame.numRows(), frame.numBytes(), 1, 0);
  }

  public ChannelCounters setTotalFiles(final long nFiles)
  {
    synchronized (this) {
      ensureCapacityForPartition(NO_PARTITION);
      totalFiles.set(NO_PARTITION, nFiles);
      return this;
    }
  }

  private void add(
      final int partitionNumber,
      final long nRows,
      final long nBytes,
      final long nFrames,
      final long nFiles
  )
  {
    synchronized (this) {
      ensureCapacityForPartition(partitionNumber);
      rows.set(partitionNumber, rows.getLong(partitionNumber) + nRows);
      bytes.set(partitionNumber, bytes.getLong(partitionNumber) + nBytes);
      frames.set(partitionNumber, frames.getLong(partitionNumber) + nFrames);
      files.set(partitionNumber, files.getLong(partitionNumber) + nFiles);
    }
  }

  @GuardedBy("this")
  private void ensureCapacityForPartition(final int partitionNumber)
  {
    while (partitionNumber >= rows.size()) {
      rows.add(0);
    }

    while (partitionNumber >= bytes.size()) {
      bytes.add(0);
    }

    while (partitionNumber >= frames.size()) {
      frames.add(0);
    }

    while (partitionNumber >= files.size()) {
      files.add(0);
    }

    while (partitionNumber >= totalFiles.size()) {
      totalFiles.add(0);
    }
  }

  @Override
  @Nullable
  public Snapshot snapshot()
  {
    final long[] rowsArray;
    final long[] bytesArray;
    final long[] framesArray;
    final long[] filesArray;
    final long[] totalFilesArray;

    synchronized (this) {
      rowsArray = listToArray(rows);
      bytesArray = listToArray(bytes);
      framesArray = listToArray(frames);
      filesArray = listToArray(files);
      totalFilesArray = listToArray(totalFiles);
    }

    if (rowsArray == null
        && bytesArray == null
        && framesArray == null
        && filesArray == null
        && totalFilesArray == null) {
      return null;
    } else {
      return new Snapshot(rowsArray, bytesArray, framesArray, filesArray, totalFilesArray);
    }
  }

  @Nullable
  private static long[] listToArray(final LongList longList)
  {
    boolean allZero = true;

    for (int i = 0; i < longList.size(); i++) {
      if (longList.getLong(i) != 0) {
        allZero = false;
        break;
      }
    }

    if (allZero) {
      return null;
    } else {
      return longList.toArray(new long[0]);
    }
  }

  @JsonTypeName("channel")
  public static class Snapshot implements QueryCounterSnapshot
  {
    private final long[] rows;
    private final long[] bytes;
    private final long[] frames;
    private final long[] files;
    private final long[] totalFiles;

    @JsonCreator
    public Snapshot(
        @Nullable @JsonProperty("rows") final long[] rows,
        @Nullable @JsonProperty("bytes") final long[] bytes,
        @Nullable @JsonProperty("frames") final long[] frames,
        @Nullable @JsonProperty("files") final long[] files,
        @Nullable @JsonProperty("totalFiles") final long[] totalFiles
    )
    {
      this.rows = rows;
      this.bytes = bytes;
      this.frames = frames;
      this.files = files;
      this.totalFiles = totalFiles;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getRows()
    {
      return rows;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getBytes()
    {
      return bytes;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getFrames()
    {
      return frames;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getFiles()
    {
      return files;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getTotalFiles()
    {
      return totalFiles;
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
      Snapshot snapshot = (Snapshot) o;
      return Arrays.equals(rows, snapshot.rows)
             && Arrays.equals(bytes, snapshot.bytes)
             && Arrays.equals(frames, snapshot.frames)
             && Arrays.equals(files, snapshot.files)
             && Arrays.equals(totalFiles, snapshot.totalFiles);
    }

    @Override
    public int hashCode()
    {
      int result = Arrays.hashCode(rows);
      result = 31 * result + Arrays.hashCode(bytes);
      result = 31 * result + Arrays.hashCode(frames);
      result = 31 * result + Arrays.hashCode(files);
      result = 31 * result + Arrays.hashCode(totalFiles);
      return result;
    }

    @Override
    public String toString()
    {
      return "ChannelCounters.Snapshot{" +
             "rows=" + Arrays.toString(rows) +
             ", bytes=" + Arrays.toString(bytes) +
             ", frames=" + Arrays.toString(frames) +
             ", files=" + Arrays.toString(files) +
             ", totalFiles=" + Arrays.toString(totalFiles) +
             '}';
    }
  }
}
