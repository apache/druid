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
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.exec.std.StandardPartitionReader;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
   * Segments currently being loaded: the acquire handle mapped to the future its delivery (or failure) completes.
   * Map membership under the queue monitor is the single terminal-ownership protocol: exactly one of
   * {@link #onSegmentReady} and {@link #close()} removes an entry and is thereafter the only party that touches
   * that handle terminally.
   */
  @GuardedBy("this")
  private final LinkedHashMap<AcquireSegmentAction, SettableFuture<ReadableInput>> loadingSegments =
      new LinkedHashMap<>();

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

  /**
   * Futures whose delivery is mid-flight: removed from {@link #loadingSegments} by {@link #onSegmentReady} but not
   * yet completed. {@link #close()} fails these too; {@link SettableFuture}'s first-write-wins semantics arbitrate
   * the race (the losing write is a harmless no-op).
   */
  @GuardedBy("this")
  private final Set<SettableFuture<ReadableInput>> inFlightDeliveries = Sets.newIdentityHashSet();

  /**
   * Set by {@link #close()}; checked by {@link #onSegmentReady} just before completing a delivery successfully.
   */
  @GuardedBy("this")
  private boolean closed = false;

  private final StandardPartitionReader partitionReader;
  private final int loadahead;

  /**
   * How segments in this queue are acquired (fully up front, or partially with on-demand column loading at query
   * time). Threaded through to {@link LoadableSegment#acquire(AcquireMode)} and
   * {@link LoadableSegment#acquireIfCached(AcquireMode)}.
   */
  private final AcquireMode acquireMode;
  private final AtomicBoolean started = new AtomicBoolean(false);

  public ReadableInputQueue(
      final StandardPartitionReader partitionReader,
      final List<PhysicalInputSlice> slices,
      final int loadahead,
      final AcquireMode acquireMode
  )
  {
    this.partitionReader = partitionReader;
    this.loadahead = loadahead;
    this.acquireMode = acquireMode;

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
          final Optional<Segment> cachedSegment = loadableSegment.acquireIfCached(acquireMode);
          if (cachedSegment.isPresent()) {
            final SegmentReferenceHolder holder = new SegmentReferenceHolder(
                new SegmentReference(loadableSegment.descriptor(), cachedSegment),
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
              readablePartition.getStageNumber(),
              readablePartition.getPartitionNumber()
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

      final SettableFuture<ReadableInput> future = SettableFuture.create();
      final AcquireSegmentAction acquireSegmentAction = nextLoadableSegment.acquire(acquireMode);
      loadingSegments.put(acquireSegmentAction, future);
      // may fire immediately on this thread for an already-ready acquire; the queue monitor is reentrant
      acquireSegmentAction.addReadyCallback(() -> onSegmentReady(nextLoadableSegment, acquireSegmentAction));
      return future;
    }
  }

  /**
   * Delivery path for {@link #loadNextSegment}: transfers ownership of the acquired segment out of the handle and
   * into a {@link SegmentReferenceHolder}, then completes the future handed to the frame processor. The future is
   * completed OUTSIDE the queue monitor so downstream listeners never run while holding the queue lock; deliveries
   * racing {@link #close()} are arbitrated by a closed re-check plus {@link SettableFuture}'s first-write-wins
   * semantics (close() also fails in-flight futures; the losing write is a harmless no-op).
   */
  private void onSegmentReady(LoadableSegment loadableSegment, AcquireSegmentAction acquireSegmentAction)
  {
    ReadableInput readableInput = null;
    Throwable failure = null;
    final SettableFuture<ReadableInput> future;

    synchronized (this) {
      future = loadingSegments.remove(acquireSegmentAction);
      if (future == null) {
        // close() already processed this handle: it closed the handle (and any delivered result) and failed the
        // future; nothing left to do.
        return;
      }
      // visible to close() as a mid-delivery future it must also fail
      inFlightDeliveries.add(future);
      // Tracks the released result until ownership transfers to a SegmentReferenceHolder in loadedSegments. If a step
      // after release() throws while this is still non-null, we own the orphaned result and must close it (close on
      // the RELEASED action is a no-op, so closing the action alone would leak the segment and its folded holds).
      AcquireSegmentResult releasedResult = null;
      try {
        // Ownership transfer; the delivered segment's close releases everything the acquire placed.
        releasedResult = acquireSegmentAction.release();
        loadableSegment.countDelivered(releasedResult);
        final SegmentReferenceHolder referenceHolder = new SegmentReferenceHolder(
            new SegmentReference(loadableSegment.descriptor(), releasedResult.getSegment()),
            loadableSegment.description()
        );
        loadedSegments.add(referenceHolder);
        // ownership is now with loadedSegments; close() will close the holder if it is never handed out
        releasedResult = null;
        readableInput = ReadableInput.segment(referenceHolder);
      }
      catch (Throwable t) {
        failure = t;
        if (releasedResult != null) {
          // release() succeeded but a later step threw before ownership transferred; close the orphaned result so its
          // segment reference and folded cache holds are released rather than leaked.
          CloseableUtils.closeAndSuppressExceptions(
              releasedResult,
              e -> log.warn(e, "Failed to close acquired segment for segment[%s]", loadableSegment.description())
          );
        } else {
          // release() itself surfaced the producer's load failure; close the handle so it reaches a terminal state
          // (a no-op close of the null result for a failed load).
          CloseableUtils.closeAndSuppressExceptions(
              acquireSegmentAction,
              e -> log.warn(e, "Failed to close acquire action for segment[%s]", loadableSegment.description())
          );
        }
      }
    }

    // Re-check for close under the monitor immediately before completing: if the queue closed after the ownership
    // work above, close() has drained (or is draining) the holder we just handed to loadedSegments, so fail the
    // future rather than delivering a dead holder. A success that lands in the last instructions before close()
    // drains can still hand the consumer a drained holder — SegmentReferenceHolder.getSegmentReferenceOnce() is the
    // designed exactly-once arbiter backstopping that pre-existing window.
    final boolean queueClosed;
    synchronized (this) {
      queueClosed = closed;
    }
    if (failure != null) {
      future.setException(failure);
    } else if (queueClosed) {
      future.setException(DruidException.defensive("Input queue closed while segment load was in flight"));
    } else {
      future.set(readableInput);
    }
    synchronized (this) {
      inFlightDeliveries.remove(future);
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
    final List<AcquireSegmentAction> handlesToClose;
    final List<SegmentReferenceHolder> holdersToDrain;
    final List<SettableFuture<ReadableInput>> futuresToFail = new ArrayList<>();

    // Snapshot and clear everything under the monitor, but do the actual closing OUTSIDE it: closing a handle can
    // synchronously run its canceler (a deferred acquire's canceler takes its own stage lock and may complete the
    // handle), and holding the queue monitor across that work would stall every concurrent delivery and nextInput()
    // call behind it. Clearing the loading map first means a late-arriving onSegmentReady finds no entry
    // (future == null) and returns without touching queue state.
    synchronized (this) {
      closed = true;
      readablePartitions.clear();
      queryableServers.clear();
      loadableSegments.clear();

      handlesToClose = new ArrayList<>(loadingSegments.keySet());
      futuresToFail.addAll(loadingSegments.values());
      loadingSegments.clear();

      // Also fail deliveries that are mid-completion (removed from loadingSegments but not yet completed);
      // SettableFuture's first-write-wins semantics make whichever write loses a harmless no-op.
      futuresToFail.addAll(inFlightDeliveries);

      holdersToDrain = new ArrayList<>(loadedSegments);
      loadedSegments.clear();

      // Drop loadahead futures that were never handed out: their loads are covered by the closing/failing here, and
      // handing them out after close would deliver holders this close() is draining. Also keeps remaining()
      // reporting 0 after close.
      pendingNextInputs.clear();
    }

    // Cancel all pending segment loads: closing a NEW handle runs its canceler (aborting the load); closing a
    // READY-but-unclaimed handle (its ready callback hasn't reached the queue monitor yet) closes the delivered
    // result.
    for (final AcquireSegmentAction acquireSegmentAction : handlesToClose) {
      CloseableUtils.closeAndSuppressExceptions(
          acquireSegmentAction,

          // AcquireSegmentAction currently doesn't have a meaningful toString method, so if this message
          // ever actually gets logged, it won't mention the specific segment that had a problem. Perhaps
          // one day this will change.
          e -> log.warn(e, "Failed to close loadingSegment[%s]", acquireSegmentAction)
      );
    }

    // Close all segments that have been loaded and not yet transferred to callers. (Segments transferred to
    // callers must be closed by the callers.)
    for (SegmentReferenceHolder referenceHolder : holdersToDrain) {
      final SegmentReference ref = referenceHolder.getSegmentReferenceOnce();
      if (ref != null) {
        CloseableUtils.closeAndSuppressExceptions(
            ref,
            e -> log.warn(e, "Failed to close loadedSegment[%s]", ref.getSegmentDescriptor())
        );
      }
    }

    // Explicitly fail the futures of the loads we just cancelled. This is load-bearing — ready callbacks are dropped
    // when a handle is closed before becoming ready, so without this the frame processors awaiting these futures
    // would hang forever.
    for (final SettableFuture<ReadableInput> future : futuresToFail) {
      future.setException(DruidException.defensive("Input queue closed while segment load was in flight"));
    }
  }
}
