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
import org.apache.druid.frame.channel.ByteTracker;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters for storage usage during MSQ query execution. Created by {@link CounterTracker#storage(ByteTracker)}.
 *
 * Tracks:
 * <ul>
 *   <li>Local byte tracker max and reserved size</li>
 *   <li>Total files and bytes written to local storage</li>
 *   <li>Total files and bytes written to durable storage</li>
 * </ul>
 */
public class StorageCounters implements QueryCounter
{
  @Nullable
  private final ByteTracker localByteTracker;

  private final AtomicLong localFilesWritten = new AtomicLong();
  private final AtomicLong localBytesWritten = new AtomicLong();
  private final AtomicLong durableFileCount = new AtomicLong();
  private final AtomicLong durableBytesWritten = new AtomicLong();

  public StorageCounters(@Nullable final ByteTracker localByteTracker)
  {
    this.localByteTracker = localByteTracker;
  }

  @Nullable
  public ByteTracker getLocalByteTracker()
  {
    return localByteTracker;
  }

  /**
   * Increments the local storage file counter by one.
   */
  public void incrementLocalFiles()
  {
    localFilesWritten.incrementAndGet();
  }

  /**
   * Adds to the local storage bytes-written counter.
   */
  public void incrementLocalBytes(final long bytes)
  {
    localBytesWritten.addAndGet(bytes);
  }

  /**
   * Increments the durable storage file counter by one.
   */
  public void incrementDurableFiles()
  {
    durableFileCount.incrementAndGet();
  }

  /**
   * Adds to the durable storage bytes-written counter.
   */
  public void incrementDurableBytes(final long bytes)
  {
    durableBytesWritten.addAndGet(bytes);
  }

  @Override
  @Nullable
  public QueryCounterSnapshot snapshot()
  {
    final Long localBytesMax;
    final long localBytesReserved;

    if (localByteTracker != null) {
      final long maxBytes = localByteTracker.getMaxBytes();
      localBytesMax = maxBytes == Long.MAX_VALUE ? null : maxBytes;
      localBytesReserved = localByteTracker.getCurrentBytes();
    } else {
      localBytesMax = null;
      localBytesReserved = 0;
    }

    return new Snapshot(
        localBytesMax,
        localBytesReserved,
        localFilesWritten.get(),
        localBytesWritten.get(),
        durableFileCount.get(),
        durableBytesWritten.get()
    );
  }

  @JsonTypeName("storage")
  public static class Snapshot implements QueryCounterSnapshot
  {
    @Nullable
    private final Long localBytesMax;
    private final long localBytesReserved;
    private final long localFilesWritten;
    private final long localBytesWritten;
    private final long durableFileCount;
    private final long durableBytesWritten;

    @JsonCreator
    public Snapshot(
        @JsonProperty("localBytesMax") @Nullable final Long localBytesMax,
        @JsonProperty("localBytesReserved") final long localBytesReserved,
        @JsonProperty("localFilesWritten") final long localFilesWritten,
        @JsonProperty("localBytesWritten") final long localBytesWritten,
        @JsonProperty("durableFileCount") final long durableFileCount,
        @JsonProperty("durableBytesWritten") final long durableBytesWritten
    )
    {
      this.localBytesMax = localBytesMax;
      this.localBytesReserved = localBytesReserved;
      this.localFilesWritten = localFilesWritten;
      this.localBytesWritten = localBytesWritten;
      this.durableFileCount = durableFileCount;
      this.durableBytesWritten = durableBytesWritten;
    }

    @Nullable
    @JsonProperty("localBytesMax")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Long getLocalBytesMax()
    {
      return localBytesMax;
    }

    @JsonProperty("localBytesReserved")
    public long getLocalBytesReserved()
    {
      return localBytesReserved;
    }

    @JsonProperty("localFilesWritten")
    public long getLocalFilesWritten()
    {
      return localFilesWritten;
    }

    @JsonProperty("localBytesWritten")
    public long getLocalBytesWritten()
    {
      return localBytesWritten;
    }

    @JsonProperty("durableFileCount")
    public long getDurableFileCount()
    {
      return durableFileCount;
    }

    @JsonProperty("durableBytesWritten")
    public long getDurableBytesWritten()
    {
      return durableBytesWritten;
    }

    @Override
    public boolean equals(final Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Snapshot snapshot = (Snapshot) o;
      return Objects.equals(localBytesMax, snapshot.localBytesMax)
             && localBytesReserved == snapshot.localBytesReserved
             && localFilesWritten == snapshot.localFilesWritten
             && localBytesWritten == snapshot.localBytesWritten
             && durableFileCount == snapshot.durableFileCount
             && durableBytesWritten == snapshot.durableBytesWritten;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(
          localBytesMax,
          localBytesReserved,
          localFilesWritten,
          localBytesWritten,
          durableFileCount,
          durableBytesWritten
      );
    }

    @Override
    public String toString()
    {
      return "Snapshot{" +
             "localBytesMax=" + localBytesMax +
             ", localBytesReserved=" + localBytesReserved +
             ", localFilesWritten=" + localFilesWritten +
             ", localBytesWritten=" + localBytesWritten +
             ", durableFileCount=" + durableFileCount +
             ", durableBytesWritten=" + durableBytesWritten +
             '}';
    }
  }
}
