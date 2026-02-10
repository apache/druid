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

package org.apache.druid.msq.querykit;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.exec.std.StandardPartitionReader;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Queue for returning {@link ReadableInput} from a list of {@link PhysicalInputSlice}.
 *
 * When closed, this object cancels all pending segment loads and releases all segments that have not yet been
 * acquired by callers through {@link SegmentReferenceHolder#getSegmentReferenceOnce()}. Callers that have acquired
 * segment references are responsible for closing those references, they will not be closed by this class.
 */
public class ReadableInputQueue implements Closeable
{
  private static final Logger log = new Logger(ReadableInputQueue.class);

  /**
   * Partitions to be read.
   */
  @GuardedBy("this")
  private final Queue<ReadablePartition> readablePartitions = new ArrayDeque<>();

  /**
   * Segments to be loaded.
   */
  @GuardedBy("this")
  private final Queue<LoadableSegment> loadableSegments = new ArrayDeque<>();

  /**
   * Realtime servers to be queried.
   */
  @GuardedBy("this")
  private final Queue<DataServerQueryHandler> queryableServers = new ArrayDeque<>();

  /**
   * Segments currently being loaded.
   */
  @GuardedBy("this")
  private final Set<AcquireSegmentAction> loadingSegments = new LinkedHashSet<>();

  /**
   * Segments that have been loaded. These are tracked here so we can close them if needed.
   */
  @GuardedBy("this")
  private final Set<SegmentReferenceHolder> loadedSegments = new LinkedHashSet<>();

  /**
   * Futures that are sitting ready to be handed out by a call to {@link #nextInput()}.
   */
  @GuardedBy("this")
  private final Set<ListenableFuture<ReadableInput>> pendingNextInputs = Sets.newIdentityHashSet();

  private final String queryId;
  private final StandardPartitionReader partitionReader;
  private final int loadahead;
  private final AtomicBoolean started = new AtomicBoolean(false);

  public ReadableInputQueue(
      final String queryId,
      final StandardPartitionReader partitionReader,
      final List<PhysicalInputSlice> slices,
      final int loadahead
  )
  {
    this.queryId = queryId;
    this.partitionReader = partitionReader;
    this.loadahead = loadahead;

    for (final PhysicalInputSlice slice : slices) {
      loadableSegments.addAll(slice.getLoadableSegments());
      queryableServers.addAll(slice.getQueryableServers());
      slice.getReadablePartitions().forEach(readablePartitions::add);
    }
  }

  /**
   * If this method has not yet been called, then:
   * (1) transition all locally-cached segments out of {@link #loadableSegments}
   * (2) start loading up to {@link #loadahead} additional segments for future calls to {@link #nextInput()}
   * If this method has previously been called, subsequent calls do nothing.
   * This is separated from the constructor because we don't want to acquire resources immediately on construction.
   */
  public void start()
  {
    if (started.compareAndSet(false, true)) {
      // (1) acquire all locally-cached segments
      synchronized (this) {
        final List<LoadableSegment> toLoad = new ArrayList<>(); // Temporarily store all non-cached segments
        LoadableSegment loadableSegment;
        while ((loadableSegment = loadableSegments.poll()) != null) {
          final Optional<Segment> cachedSegment = loadableSegment.acquireIfCached();
          if (cachedSegment.isPresent()) {
            final SegmentReferenceHolder holder = new SegmentReferenceHolder(
                new SegmentReference(loadableSegment.descriptor(), cachedSegment, null),
                loadableSegment.inputCounters(),
                loadableSegment.description()
            );
            loadedSegments.add(holder);
            pendingNextInputs.add(Futures.immediateFuture(ReadableInput.segment(holder)));
          } else {
            toLoad.add(loadableSegment);
          }
        }
        loadableSegments.addAll(toLoad); // Put non-cached segments back into loadableSegments
      }

      // (2) start loading up to "loadahead" additional segments
      for (int i = 0; i < loadahead; i++) {
        if (!addLoadaheadFuture()) {
          break;
        }
      }
    }
  }

  /**
   * Returns the number of remaining inputs that can be returned by calls to {@link #nextInput()}.
   */
  public int remaining()
  {
    synchronized (this) {
      return readablePartitions.size() + loadableSegments.size() + queryableServers.size() + pendingNextInputs.size();
    }
  }

  /**
   * Returns the next {@link ReadableInput}. The future resolves when the input is ready to read.
   */
  @Nullable
  public ListenableFuture<ReadableInput> nextInput()
  {
    if (!started.get()) {
      throw DruidException.defensive("Not started, must call start() first");
    }

    ListenableFuture<ReadableInput> future;

    future = nextServerInput();
    if (future != null) {
      return future;
    }

    future = nextChannelInput();
    if (future != null) {
      return future;
    }

    future = nextSegmentInput();
    if (future != null) {
      return future;
    }

    return null;
  }

  /**
   * Returns the next input from {@link #queryableServers}, if any. Returns null if none remain.
   */
  @Nullable
  private ListenableFuture<ReadableInput> nextServerInput()
  {
    final DataServerQueryHandler handler;
    synchronized (this) {
      handler = queryableServers.poll();
    }

    if (handler == null) {
      return null;
    }

    return Futures.immediateFuture(ReadableInput.dataServerQuery(handler));
  }

  /**
   * Returns the next input from {@link #readablePartitions}, if any. Returns null if none remain.
   */
  @Nullable
  private ListenableFuture<ReadableInput> nextChannelInput()
  {
    final ReadablePartition readablePartition;
    synchronized (this) {
      readablePartition = readablePartitions.poll();
    }

    if (readablePartition == null) {
      return null;
    }

    ReadableFrameChannel channel = null;
    try {
      channel = partitionReader.openChannel(readablePartition);
      return Futures.immediateFuture(
          ReadableInput.channel(
              channel,
              partitionReader.frameReader(readablePartition.getStageNumber()),
              new StagePartition(
                  new StageId(queryId, readablePartition.getStageNumber()),
                  readablePartition.getPartitionNumber()
              )
          )
      );
    }
    catch (IOException e) {
      throw CloseableUtils.closeAndWrapInCatch(e, channel);
    }
  }

  /**
   * Returns the next input from {@link #loadableSegments}, if any. Returns null if none remain.
   */
  @Nullable
  private ListenableFuture<ReadableInput> nextSegmentInput()
  {
    // Pick a loadahead future, preferring ones that are already loaded.
    ListenableFuture<ReadableInput> selectedLoadaheadFuture = null;
    synchronized (this) {
      for (ListenableFuture<ReadableInput> f : pendingNextInputs) {
        if (selectedLoadaheadFuture == null || f.isDone()) {
          selectedLoadaheadFuture = f;
          if (f.isDone()) {
            break;
          }
        }
      }

      if (selectedLoadaheadFuture != null) {
        pendingNextInputs.remove(selectedLoadaheadFuture);
        if (pendingNextInputs.size() < loadahead) {
          addLoadaheadFuture(); // Replace the one we just took out.
        }
        return selectedLoadaheadFuture;
      }
    }

    return loadNextSegment();
  }

  /**
   * Load the next segment from {@link #loadableSegments} and return a future to its reference. Returns null
   * if {@link #loadableSegments} is empty.
   */
  @Nullable
  private ListenableFuture<ReadableInput> loadNextSegment()
  {
    synchronized (this) {
      final LoadableSegment nextLoadableSegment = loadableSegments.poll();
      if (nextLoadableSegment == null) {
        return null;
      }

      final AcquireSegmentAction acquireSegmentAction = nextLoadableSegment.acquire();
      loadingSegments.add(acquireSegmentAction);
      return FutureUtils.transform(
          acquireSegmentAction.getSegmentFuture(),
          segment -> {
            synchronized (ReadableInputQueue.this) {
              // Transfer segment from "loadingSegments" to "loadedSegments" and return a reference to it.
              if (loadingSegments.remove(acquireSegmentAction)) {
                try {
                  final ChannelCounters inputCounters = nextLoadableSegment.inputCounters();
                  if (inputCounters != null) {
                    inputCounters.addLoad(segment);
                  }
                  final SegmentReferenceHolder referenceHolder = new SegmentReferenceHolder(
                      new SegmentReference(
                          nextLoadableSegment.descriptor(),
                          segment.getReferenceProvider().acquireReference(),
                          acquireSegmentAction // Release the hold when the SegmentReference is closed.
                      ),
                      nextLoadableSegment.inputCounters(),
                      nextLoadableSegment.description()
                  );
                  loadedSegments.add(referenceHolder);
                  return ReadableInput.segment(referenceHolder);
                }
                catch (Throwable e) {
                  // Javadoc for segment.acquireReference() suggests it can throw exceptions; handle that here
                  // by closing the original AcquireSegmentAction.
                  throw CloseableUtils.closeAndWrapInCatch(e, acquireSegmentAction);
                }
              } else {
                throw DruidException.defensive(
                    "Segment[%s] removed from loadingSegments before loading complete. It is possible that close() "
                    + "was called with futures in flight.",
                    nextLoadableSegment.description()
                );
              }
            }
          }
      );
    }
  }

  /**
   * Calls {@link #nextSegmentInput()} and adds the future to {@link #pendingNextInputs}. Returns whether a future
   * was added.
   */
  private boolean addLoadaheadFuture()
  {
    final ListenableFuture<ReadableInput> nextFuture = loadNextSegment();
    if (nextFuture != null) {
      synchronized (this) {
        pendingNextInputs.add(nextFuture);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void close()
  {
    synchronized (this) {
      readablePartitions.clear();
      queryableServers.clear();
      loadableSegments.clear();

      // Cancel all pending segment loads.
      for (AcquireSegmentAction acquireSegmentAction : loadingSegments) {
        CloseableUtils.closeAndSuppressExceptions(
            acquireSegmentAction,

            // AcquireSegmentAction currently doesn't have a meaningful toString method, so if this message
            // ever actually gets logged, it won't mention the specific segment that had a problem. Perhaps
            // one day this will change.
            e -> log.warn(e, "Failed to close loadingSegment[%s]", acquireSegmentAction)
        );
      }
      loadingSegments.clear();

      // Close all segments that have been loaded and not yet transferred to callers. (Segments transferred to
      // callers must be closed by the callers.)
      for (SegmentReferenceHolder referenceHolder : loadedSegments) {
        final SegmentReference ref = referenceHolder.getSegmentReferenceOnce();
        if (ref != null) {
          CloseableUtils.closeAndSuppressExceptions(
              ref,
              e -> log.warn(e, "Failed to close loadedSegment[%s]", ref.getSegmentDescriptor())
          );
        }
      }
      loadedSegments.clear();
    }
  }
}
