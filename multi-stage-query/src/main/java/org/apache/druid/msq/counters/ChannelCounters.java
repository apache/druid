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
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.loading.AcquireSegmentResult;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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

  @GuardedBy("this")
  private final LongList loadFiles = new LongArrayList();

  @GuardedBy("this")
  private final LongList loadBytes = new LongArrayList();

  @GuardedBy("this")
  private final LongList loadTime = new LongArrayList();

  @GuardedBy("this")
  private final LongList loadWait = new LongArrayList();

  public void incrementRowCount()
  {
    incrementRowCount(NO_PARTITION);
  }

  public void incrementRowCount(int partition)
  {
    add(partition, 1, 0, 0, 0);
  }

  public void incrementBytes(long bytes)
  {
    add(NO_PARTITION, 0, bytes, 0, 0);
  }

  public void incrementFileCount()
  {
    add(NO_PARTITION, 0, 0, 0, 1);
  }

  public void addFile(final long nRows, final long nBytes)
  {
    add(NO_PARTITION, nRows, nBytes, 0, 1);
  }

  public void addLoad(AcquireSegmentResult loadResult)
  {
    if (loadResult.getLoadSizeBytes() > 0) {
      addLoad(
          NO_PARTITION,
          loadResult.getLoadSizeBytes(),
          TimeUnit.NANOSECONDS.toMillis(loadResult.getLoadTimeNanos()),
          TimeUnit.NANOSECONDS.toMillis(loadResult.getWaitTimeNanos()),
          1
      );
    }
  }

  /**
   * Increment counts by one frame, and if this {@link RowsAndColumns} is a {@link Frame}, also increment
   * bytes by {@link Frame#numBytes()}.
   */
  public void addRAC(final RowsAndColumns rac, final int partitionNumber)
  {
    final Frame frame = rac.as(Frame.class);
    final long numBytes = frame != null ? frame.numBytes() : 0;
    add(partitionNumber, rac.numRows(), numBytes, 1, 0);
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

  private void addLoad(
      final int partitionNumber,
      final long nBytes,
      final long nTime,
      final long nWait,
      final long nFiles
  )
  {
    synchronized (this) {
      ensureCapacityForPartitionLoad(partitionNumber);
      loadBytes.set(partitionNumber, loadBytes.getLong(partitionNumber) + nBytes);
      loadTime.set(partitionNumber, loadTime.getLong(partitionNumber) + nTime);
      loadWait.set(partitionNumber, loadWait.getLong(partitionNumber) + nWait);
      loadFiles.set(partitionNumber, loadFiles.getLong(partitionNumber) + nFiles);
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

  @GuardedBy("this")
  private void ensureCapacityForPartitionLoad(final int partitionNumber)
  {
    while (partitionNumber >= loadFiles.size()) {
      loadFiles.add(0);
    }

    while (partitionNumber >= loadBytes.size()) {
      loadBytes.add(0);
    }

    while (partitionNumber >= loadTime.size()) {
      loadTime.add(0);
    }

    while (partitionNumber >= loadWait.size()) {
      loadWait.add(0);
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
    final long[] loadBytesArray;
    final long[] loadTimeArray;
    final long[] loadWaitArray;
    final long[] loadFilesArray;

    synchronized (this) {
      rowsArray = listToArray(rows);
      bytesArray = listToArray(bytes);
      framesArray = listToArray(frames);
      filesArray = listToArray(files);
      totalFilesArray = listToArray(totalFiles);
      loadBytesArray = listToArray(loadBytes);
      loadTimeArray = listToArray(loadTime);
      loadWaitArray = listToArray(loadWait);
      loadFilesArray = listToArray(loadFiles);
    }

    if (rowsArray == null
        && bytesArray == null
        && framesArray == null
        && filesArray == null
        && totalFilesArray == null
        && loadBytesArray == null
        && loadTimeArray == null
        && loadWaitArray == null
        && loadFilesArray == null
    ) {
      return null;
    } else {
      return new Snapshot(
          rowsArray,
          bytesArray,
          framesArray,
          filesArray,
          totalFilesArray,
          loadBytesArray,
          loadTimeArray,
          loadWaitArray,
          loadFilesArray
      );
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
    private final long[] loadBytes;
    private final long[] loadTime;
    private final long[] loadWait;
    private final long[] loadFiles;

    @JsonCreator
    public Snapshot(
        @Nullable @JsonProperty("rows") final long[] rows,
        @Nullable @JsonProperty("bytes") final long[] bytes,
        @Nullable @JsonProperty("frames") final long[] frames,
        @Nullable @JsonProperty("files") final long[] files,
        @Nullable @JsonProperty("totalFiles") final long[] totalFiles,
        @Nullable @JsonProperty("loadBytes") final long[] loadBytes,
        @Nullable @JsonProperty("loadTime") final long[] loadTime,
        @Nullable @JsonProperty("loadWait") final long[] loadWait,
        @Nullable @JsonProperty("loadFiles") final long[] loadFiles
    )
    {
      this.rows = rows;
      this.bytes = bytes;
      this.frames = frames;
      this.files = files;
      this.totalFiles = totalFiles;
      this.loadBytes = loadBytes;
      this.loadTime = loadTime;
      this.loadWait = loadWait;
      this.loadFiles = loadFiles;
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

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getLoadBytes()
    {
      return loadBytes;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getLoadTime()
    {
      return loadTime;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getLoadWait()
    {
      return loadWait;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long[] getLoadFiles()
    {
      return loadFiles;
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
             && Arrays.equals(totalFiles, snapshot.totalFiles)
             && Arrays.equals(loadBytes, snapshot.loadBytes)
             && Arrays.equals(loadTime, snapshot.loadTime)
             && Arrays.equals(loadWait, snapshot.loadWait)
             && Arrays.equals(loadFiles, snapshot.loadFiles);
    }

    @Override
    public int hashCode()
    {
      int result = Arrays.hashCode(rows);
      result = 31 * result + Arrays.hashCode(bytes);
      result = 31 * result + Arrays.hashCode(frames);
      result = 31 * result + Arrays.hashCode(files);
      result = 31 * result + Arrays.hashCode(totalFiles);
      result = 31 * result + Arrays.hashCode(loadBytes);
      result = 31 * result + Arrays.hashCode(loadTime);
      result = 31 * result + Arrays.hashCode(loadWait);
      result = 31 * result + Arrays.hashCode(loadFiles);
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
             ", loadBytes=" + Arrays.toString(loadBytes) +
             ", loadTime=" + Arrays.toString(loadTime) +
             ", loadWait=" + Arrays.toString(loadWait) +
             ", loadFiles=" + Arrays.toString(loadFiles) +
             '}';
    }
  }
}
