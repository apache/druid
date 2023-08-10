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

package org.apache.druid.msq.exec;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Production implementation of {@link DataSegmentProvider} using Coordinator APIs.
 */
public class TaskDataSegmentProvider implements DataSegmentProvider
{
  private final CoordinatorClient coordinatorClient;
  private final SegmentCacheManager segmentCacheManager;
  private final IndexIO indexIO;
  private final ConcurrentHashMap<SegmentId, SegmentHolder> holders;

  public TaskDataSegmentProvider(
      CoordinatorClient coordinatorClient,
      SegmentCacheManager segmentCacheManager,
      IndexIO indexIO
  )
  {
    this.coordinatorClient = coordinatorClient;
    this.segmentCacheManager = segmentCacheManager;
    this.indexIO = indexIO;
    this.holders = new ConcurrentHashMap<>();
  }

  @Override
  public Supplier<ResourceHolder<Segment>> fetchSegment(
      final SegmentId segmentId,
      final ChannelCounters channelCounters
  )
  {
    // Returns Supplier<ResourceHolder> instead of ResourceHolder, so the Coordinator calls and segment downloads happen
    // in processing threads, rather than the main thread. (They happen when fetchSegmentInternal is called.)
    return () -> {
      ResourceHolder<Segment> holder = null;

      while (holder == null) {
        holder = holders.computeIfAbsent(
            segmentId,
            k -> new SegmentHolder(
                () -> fetchSegmentInternal(segmentId, channelCounters),
                () -> holders.remove(segmentId)
            )
        ).get();
      }

      return holder;
    };
  }

  /**
   * Helper used by {@link #fetchSegment(SegmentId, ChannelCounters)}. Does the actual fetching of a segment, once it
   * is determined that we definitely need to go out and get one.
   */
  private ReferenceCountingResourceHolder<Segment> fetchSegmentInternal(
      final SegmentId segmentId,
      final ChannelCounters channelCounters
  )
  {
    final DataSegment dataSegment;
    try {
      dataSegment = FutureUtils.get(
          coordinatorClient.fetchUsedSegment(
              segmentId.getDataSource(),
              segmentId.toString()
          ),
          true
      );
    }
    catch (InterruptedException | ExecutionException e) {
      throw new RE(e, "Failed to fetch segment details from Coordinator for [%s]", segmentId);
    }

    final Closer closer = Closer.create();
    try {
      if (!segmentCacheManager.reserve(dataSegment)) {
        throw new ISE("Could not reserve location for segment [%s]", segmentId);
      }
      closer.register(() -> segmentCacheManager.cleanup(dataSegment));
      final File segmentDir = segmentCacheManager.getSegmentFiles(dataSegment);

      final QueryableIndex index = closer.register(indexIO.loadIndex(segmentDir));
      final QueryableIndexSegment segment = new QueryableIndexSegment(index, dataSegment.getId());
      final int numRows = index.getNumRows();
      final long size = dataSegment.getSize();
      closer.register(() -> channelCounters.addFile(numRows, size));
      return new ReferenceCountingResourceHolder<>(segment, closer);
    }
    catch (IOException | SegmentLoadingException e) {
      throw CloseableUtils.closeInCatch(
          new RE(e, "Failed to download segment [%s]", segmentId),
          closer
      );
    }
  }

  private static class SegmentHolder implements Supplier<ResourceHolder<Segment>>
  {
    private final Supplier<ResourceHolder<Segment>> holderSupplier;
    private final Closeable cleanupFn;

    @GuardedBy("this")
    private ReferenceCountingResourceHolder<Segment> holder;

    @GuardedBy("this")
    private boolean closing;

    @GuardedBy("this")
    private boolean closed;

    public SegmentHolder(Supplier<ResourceHolder<Segment>> holderSupplier, Closeable cleanupFn)
    {
      this.holderSupplier = holderSupplier;
      this.cleanupFn = cleanupFn;
    }

    @Override
    @Nullable
    public ResourceHolder<Segment> get()
    {
      synchronized (this) {
        if (closing) {
          // Wait until the holder is closed.
          while (!closed) {
            try {
              wait();
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          }

          // Then, return null so "fetchSegment" will try again.
          return null;
        } else if (holder == null) {
          final ResourceHolder<Segment> segmentHolder = holderSupplier.get();
          holder = new ReferenceCountingResourceHolder<>(
              segmentHolder.get(),
              () -> {
                synchronized (this) {
                  CloseableUtils.closeAll(
                      () -> {
                        // synchronized block not strictly needed here, but errorprone needs it since it doesn't
                        // understand the lambda is immediately called. See https://errorprone.info/bugpattern/GuardedBy
                        synchronized (this) {
                          closing = true;
                        }
                      },
                      segmentHolder,
                      cleanupFn, // removes this holder from the "holders" map
                      () -> {
                        // synchronized block not strictly needed here, but errorprone needs it since it doesn't
                        // understand the lambda is immediately called. See https://errorprone.info/bugpattern/GuardedBy
                        synchronized (this) {
                          closed = true;
                          SegmentHolder.this.notifyAll();
                        }
                      }
                  );
                }
              }
          );
          final ResourceHolder<Segment> retVal = holder.increment();
          // Store already-closed holder, so it disappears when the last reference is closed.
          holder.close();
          return retVal;
        } else {
          try {
            return holder.increment();
          }
          catch (IllegalStateException e) {
            // Possible race: holder is in the process of closing. (This is the only reason "increment" can throw ISE.)
            // Return null so "fetchSegment" will try again.
            return null;
          }
        }
      }
    }
  }
}
