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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.exec.std.StandardPartitionReader;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.query.Order;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.external.VirtualStorageManager;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Queue for returning {@link ReadableInput} from a list of {@link PhysicalInputSlice}.
 *
 * When closed, this object cancels all pending segment loads and releases all segments that have not yet been
 * acquired by callers through {@link SegmentReferenceHolder#getSegmentReferenceOnce()}. Callers that have acquired
 * segment references are responsible for closing those references, they will not be closed by this class.
 *
 * Load-ahead (see {@link #loadahead}) is a prefetch, so a full segment cache alone does not make it fail a query: when
 * a load-ahead acquisition finds no room in the segment cache before it has been handed out, the segment goes back into
 * {@link #loadableSegments} and load-ahead backs off for a while (see {@link #deferLoadahead}). Other failures do fail
 * the query: a load-ahead that runs out of room after it was handed out (a caller is waiting on it), a load-ahead that
 * fails for any other reason, and a segment acquired on demand by {@link #nextSegmentInput()}, which is needed to make
 * progress right now.
 */
public class ReadableInputQueue implements Closeable
{
  private static final Logger log = new Logger(ReadableInputQueue.class);

  /**
   * Largest value of {@link #loadaheadCooldown}, i.e. the longest load-ahead backs off for when the segment cache
   * stays full.
   */
  private static final int MAX_LOADAHEAD_COOLDOWN = 64;

  /**
   * Partitions to be read.
   */
  @GuardedBy("this")
  private final Queue<NumberedItem<ReadablePartition>> readablePartitions = new ArrayDeque<>();

  /**
   * Segments to be loaded, in the order they are handed out, i.e. sorted by {@link QueuedSegment#position()}. Segments
   * are taken from the front when they are acquired, and a segment whose load-ahead acquisition could not be
   * accommodated by the segment cache is put back in its place by {@link #deferLoadahead}.
   */
  @GuardedBy("this")
  private final Deque<QueuedSegment> loadableSegments = new ArrayDeque<>();

  /**
   * Realtime servers to be queried.
   */
  @GuardedBy("this")
  private final Queue<NumberedItem<DataServerQueryHandler>> queryableServers = new ArrayDeque<>();

  /**
   * Segments currently being loaded: the acquire handle mapped to the future its delivery (or failure) completes.
   */
  @GuardedBy("this")
  private final LinkedHashMap<AcquireSegmentAction, SettableFuture<NumberedReadableInput>> loadingSegments =
      new LinkedHashMap<>();

  /**
   * Segments that have been loaded. These are tracked here so we can close them if needed.
   */
  @GuardedBy("this")
  private final Set<SegmentReferenceHolder> loadedSegments = new LinkedHashSet<>();

  /**
   * Futures waiting to be handed out by a call to {@link #nextInput()}, some still loading, mapped to the input each
   * one delivers, in the order their acquisition started.
   */
  @GuardedBy("this")
  private final LinkedHashMap<ListenableFuture<NumberedReadableInput>, PendingInput> pendingNextInputs =
      new LinkedHashMap<>();

  /**
   * Number of entries in {@link #pendingNextInputs} started by load-ahead, i.e. not {@link PendingInput#cached()}.
   * Load-ahead keeps this topped up to {@link #loadahead}.
   */
  @GuardedBy("this")
  private int pendingLoadaheadCount;

  /**
   * Number of upcoming load-ahead opportunities to skip, because the segment cache had no room for a recent
   * load-ahead acquisition. Counts down one per input handed out by {@link #nextSegmentInput()}, which paces it by the
   * progress of the stage.
   */
  @GuardedBy("this")
  private int loadaheadCooldown;

  /**
   * Length of the latest {@link #loadaheadCooldown}. Doubles (up to {@link #MAX_LOADAHEAD_COOLDOWN}) each time a
   * load-ahead acquisition is deferred while no cooldown is running, and resets to zero once one succeeds, so a cache
   * that stays full does not turn load-ahead into a stream of doomed acquisitions, yet load-ahead resumes at full speed
   * once space frees up. A deferral during a cooldown does not double it again, since it comes from an acquisition that
   * was already in flight when the cooldown started: several load-aheads failing together count as one failed round.
   */
  @GuardedBy("this")
  private int loadaheadBackoff;

  /**
   * Whether we have logged about deferring load-ahead. Used to log the first deferral for this queue at INFO and the
   * rest at DEBUG, since a full segment cache typically defers load-ahead over and over.
   */
  @GuardedBy("this")
  private boolean loggedLoadaheadDeferral;

  /**
   * Interval of the last group probed by {@link #acquireCachedInNextGroup()}. Intervals in the same group compare equal
   * under {@link #intervalOrder}.
   */
  @GuardedBy("this")
  @Nullable
  private Interval probedGroupInterval;

  /**
   * Futures whose delivery is mid-flight and have been removed from {@link #loadingSegments} by {@link #onSegmentReady}
   * but not yet completed ({@link #close()} fails these too).
   */
  @GuardedBy("this")
  private final Set<SettableFuture<NumberedReadableInput>> inFlightDeliveries = Sets.newIdentityHashSet();

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

  /**
   * Decides which inputs can be skipped as well as which order (if any) they should be processed in.
   */
  private final InputPruner inputPruner;

  /**
   * Time order in which segments are handed out, from {@link InputPruner#getTimeOrder()}.
   */
  private final Order timeOrder;

  /**
   * Orders segment intervals by {@link #timeOrder}. Intervals that can hold the best rows come first.
   */
  private final Comparator<Interval> intervalOrder;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Creates a new queue.
   *
   * @param partitionReader Reader for {@link StageInputSlice}
   * @param inputNumbers array of input numbers, one per input
   * @param slices array of slices, one per input (same length as inputNumbers)
   * @param loadahead number of items to load ahead of the queue
   * @param acquireMode acquire mode for {@link SegmentCacheManager}
   * @param inputPruner decides which inputs can be skipped; see {@link #inputPruner}
   */
  public ReadableInputQueue(
      final StandardPartitionReader partitionReader,
      final IntList inputNumbers,
      final List<PhysicalInputSlice> slices,
      final int loadahead,
      final AcquireMode acquireMode,
      final InputPruner inputPruner
  )
  {
    this.partitionReader = partitionReader;
    this.loadahead = loadahead;
    this.acquireMode = acquireMode;
    this.inputPruner = inputPruner;
    this.timeOrder = inputPruner.getTimeOrder();
    this.intervalOrder = makeIntervalOrder(timeOrder);

    if (inputNumbers.size() != slices.size()) {
      throw DruidException.defensive("Got input numbers[%s] for slices[%s]", inputNumbers, slices);
    }

    final List<NumberedItem<LoadableSegment>> segments = new ArrayList<>();
    for (int i = 0; i < slices.size(); i++) {
      final int inputNumber = inputNumbers.getInt(i);
      final PhysicalInputSlice slice = slices.get(i);

      for (final LoadableSegment segment : slice.getLoadableSegments()) {
        segments.add(new NumberedItem<>(inputNumber, segment));
      }

      for (final DataServerQueryHandler server : slice.getQueryableServers()) {
        queryableServers.add(new NumberedItem<>(inputNumber, server));
      }

      for (final ReadablePartition partition : slice.getReadablePartitions()) {
        readablePartitions.add(new NumberedItem<>(inputNumber, partition));
      }
    }

    if (timeOrder != Order.NONE) {
      segments.sort(Comparator.comparing(item -> item.item().descriptor().getInterval(), intervalOrder));
    }
    for (int i = 0; i < segments.size(); i++) {
      final NumberedItem<LoadableSegment> segment = segments.get(i);
      loadableSegments.add(new QueuedSegment(i, segment.inputNumber(), segment.item()));
    }
  }

  /**
   * If this method has not yet been called, then:
   * (1) acquire the locally-cached segments of the first interval group, see {@link #acquireCachedInNextGroup()}. When
   * {@link #timeOrder} is {@link Order#NONE}, that is all locally-cached segments.
   * (2) start loading up to {@link #loadahead} additional segments for future calls to {@link #nextInput()}
   * If this method has previously been called, subsequent calls do nothing.
   * This is separated from the constructor because we don't want to acquire resources immediately on construction.
   */
  public void start()
  {
    if (started.compareAndSet(false, true)) {
      // (1) acquire the locally-cached segments of the first interval group
      synchronized (this) {
        acquireCachedInNextGroup();
      }

      // (2) start loading up to "loadahead" additional segments. Stops short if the segment cache cannot accommodate
      // them all; the rest are acquired later, either by a subsequent load-ahead or on demand.
      topUpLoadahead();
    }
  }

  /**
   * Returns an upper bound on the number of remaining inputs that can be returned by calls to {@link #nextInput()}.
   * Inputs that {@link #inputPruner} will rule out are counted until they are reached.
   */
  public int remaining()
  {
    synchronized (this) {
      return readablePartitions.size() + loadableSegments.size() + queryableServers.size() + pendingNextInputs.size();
    }
  }

  /**
   * Returns the next {@link NumberedReadableInput}. The future resolves when the input is ready to read.
   */
  @Nullable
  public ListenableFuture<NumberedReadableInput> nextInput()
  {
    if (!started.get()) {
      throw DruidException.defensive("Not started, must call start() first");
    }

    if (canSkipRemaining()) {
      final List<ListenableFuture<NumberedReadableInput>> skippedFutures;
      synchronized (this) {
        skippedFutures = new ArrayList<>(pendingNextInputs.keySet());
        pendingNextInputs.clear();
        pendingLoadaheadCount = 0;
      }
      releaseSkippedInputs(skippedFutures);
      return null;
    }

    ListenableFuture<NumberedReadableInput> future;

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
  private ListenableFuture<NumberedReadableInput> nextServerInput()
  {
    final NumberedItem<DataServerQueryHandler> handler;
    synchronized (this) {
      handler = queryableServers.poll();
    }

    if (handler == null) {
      return null;
    }

    return Futures.immediateFuture(
        new NumberedReadableInput(handler.inputNumber(), ReadableInput.dataServerQuery(handler.item()))
    );
  }

  /**
   * Returns the next input from {@link #readablePartitions}, if any. Returns null if none remain.
   */
  @Nullable
  private ListenableFuture<NumberedReadableInput> nextChannelInput()
  {
    final NumberedItem<ReadablePartition> nextItem;
    synchronized (this) {
      nextItem = readablePartitions.poll();
    }

    if (nextItem == null) {
      return null;
    }

    final ReadablePartition readablePartition = nextItem.item();
    ReadableFrameChannel channel = null;
    try {
      channel = partitionReader.openChannel(readablePartition);
      return Futures.immediateFuture(
          new NumberedReadableInput(
              nextItem.inputNumber(),
              ReadableInput.channel(
                  channel,
                  partitionReader.frameReader(readablePartition.getStageNumber()),
                  readablePartition.getStageNumber(),
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
   * Returns the next input from {@link #loadableSegments}, if any: a pending input if there is one, else a segment
   * acquired on demand. Returns null if none remain.
   */
  @Nullable
  private ListenableFuture<NumberedReadableInput> nextSegmentInput()
  {
    final List<ListenableFuture<NumberedReadableInput>> skippedFutures = new ArrayList<>();
    ListenableFuture<NumberedReadableInput> unskippedFuture = null;
    LoadableSegment unskippedSegment = null;

    synchronized (this) {
      acquireCachedInNextGroup();
      while (unskippedFuture == null && !pendingNextInputs.isEmpty()) {
        final ListenableFuture<NumberedReadableInput> selectedFuture = selectPendingInput();
        final PendingInput selectedInput = removePendingInput(selectedFuture);
        // The selected segment may have been pruned since its load was started, check if it can be skipped once more
        // before handing it out.
        if (canSkipSegment(selectedInput.segment())) {
          skippedFutures.add(selectedFuture);
          // Skipping may have moved the next input to a new interval group.
          acquireCachedInNextGroup();
        } else {
          unskippedFuture = selectedFuture;
          unskippedSegment = selectedInput.segment();
        }
      }
    }

    releaseSkippedInputs(skippedFutures);

    final ListenableFuture<NumberedReadableInput> future;
    final LoadableSegment segment;
    if (unskippedFuture != null) {
      future = unskippedFuture;
      segment = unskippedSegment;
    } else {
      // Nothing unpruned is pending (loadahead is 0, load-ahead is backing off, or every pending input was skipped), so
      // acquire on demand. The caller has nothing else to work on, which makes this segment part of what the stage
      // needs in order to make progress so running out of room in the segment cache is fatal here.
      final Acquisition acquisition;
      synchronized (this) {
        final QueuedSegment nextSegment = pollUnprunedSegment();
        if (nextSegment == null) {
          return null;
        }
        acquisition = acquireSegment(nextSegment, false);
      }
      registerReadyCallback(acquisition);
      future = acquisition.future();
      segment = acquisition.segment();
    }

    // Top up load-ahead before registering the count: if that throws, the future is never handed out, so it must not
    // be counted as read.
    afterHandOut();
    registerCountOnSuccess(future, segment);
    return future;
  }

  /**
   * Called once per input handed out by {@link #nextSegmentInput()}, which is what paces load-ahead. Burns off one
   * unit of {@link #loadaheadCooldown} if load-ahead is backing off, else tops load-ahead back up, which replaces the
   * input just handed out if it was itself a load-ahead.
   */
  private void afterHandOut()
  {
    synchronized (this) {
      if (loadaheadCooldown > 0) {
        loadaheadCooldown--;
        return;
      }
    }
    topUpLoadahead();
  }

  /**
   * Starts load-ahead acquisitions until {@link #pendingLoadaheadCount} reaches {@link #loadahead}. Stops early when
   * the queue is closed, a {@link #loadaheadCooldown} is running, {@link #inputPruner} rules out every remaining input,
   * no segments remain to be acquired, or an acquisition is deferred for lack of room in the segment cache (which
   * starts a cooldown).
   * <p>
   * Acquires one segment at a time, registering its ready callback before acquiring the next: the callback fires
   * inline for an already-ready acquisition, so a no-room failure that is known right away starts the cooldown.
   */
  private void topUpLoadahead()
  {
    while (true) {
      final Acquisition acquisition;
      synchronized (this) {
        if (closed || loadaheadCooldown > 0 || pendingLoadaheadCount >= loadahead) {
          return;
        }
        acquisition = addLoadaheadFuture();
      }
      if (acquisition == null) {
        return;
      }
      registerReadyCallback(acquisition);
    }
  }

  /**
   * Registers a callback that counts {@code segment} as read once {@code future} succeeds, which is when the input is
   * handed off for processing.
   */
  private static void registerCountOnSuccess(
      final ListenableFuture<NumberedReadableInput> future,
      final LoadableSegment segment
  )
  {
    Futures.addCallback(
        future,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(final NumberedReadableInput result)
          {
            segment.countHandedOut();
          }

          @Override
          public void onFailure(final Throwable t)
          {
            // Failed inputs are never processed, so there is nothing to count.
          }
        },
        MoreExecutors.directExecutor()
    );
  }

  /**
   * Picks the pending input to hand out next. Never null; only called when {@link #pendingNextInputs} is nonempty.
   */
  @GuardedBy("this")
  private ListenableFuture<NumberedReadableInput> selectPendingInput()
  {
    final Iterator<Map.Entry<ListenableFuture<NumberedReadableInput>, PendingInput>> iterator =
        pendingNextInputs.entrySet().iterator();
    final Map.Entry<ListenableFuture<NumberedReadableInput>, PendingInput> head = iterator.next();
    if (head.getKey().isDone()) {
      // Eagerly hand out a ready head input.
      return head.getKey();
    }

    // head is not ready, seek for the next ready input who compares equal to head. for ordered inputs that means
    // matching intervals, for unordered inputs any ready input is fine.
    final Interval headInterval = head.getValue().interval();
    while (iterator.hasNext()) {
      final Map.Entry<ListenableFuture<NumberedReadableInput>, PendingInput> entry = iterator.next();
      if (entry.getKey().isDone() && intervalOrder.compare(headInterval, entry.getValue().interval()) == 0) {
        return entry.getKey();
      }
    }

    // Nothing level with the head is loaded, so hand out the head and let the caller wait for it.
    return head.getKey();
  }

  /**
   * Acquires the locally-cached segments of the next interval group to be handed out and adds them to
   * {@link #pendingNextInputs}, once per group.
   * <p>
   * A group is the leading run of {@link #loadableSegments} whose intervals compare equal under {@link #intervalOrder};
   * with {@link Order#NONE}, that is every segment. Segments that {@link #inputPruner} rules out are dropped.
   */
  @GuardedBy("this")
  private void acquireCachedInNextGroup()
  {
    final Interval nextInterval;
    if (!pendingNextInputs.isEmpty()) {
      nextInterval = pendingNextInputs.values().iterator().next().interval();
    } else if (!loadableSegments.isEmpty()) {
      nextInterval = loadableSegments.peek().segment().descriptor().getInterval();
    } else {
      return;
    }

    if (probedGroupInterval != null && intervalOrder.compare(probedGroupInterval, nextInterval) == 0) {
      return;
    }
    probedGroupInterval = nextInterval;

    final List<QueuedSegment> notCached = new ArrayList<>();
    try {
      QueuedSegment nextSegment;
      while ((nextSegment = loadableSegments.peek()) != null
             && intervalOrder.compare(nextInterval, nextSegment.segment().descriptor().getInterval()) == 0) {
        loadableSegments.poll();
        if (canSkipSegment(nextSegment.segment())) {
          continue;
        }
        final ListenableFuture<NumberedReadableInput> cachedFuture;
        try {
          cachedFuture = acquireIfCached(nextSegment);
        }
        catch (Throwable t) {
          // acquireIfCached leaves the segment unacquired when it throws, so it goes back with the others.
          notCached.add(nextSegment);
          throw t;
        }
        if (cachedFuture != null) {
          pendingNextInputs.put(cachedFuture, new PendingInput(nextSegment.segment(), true));
        } else {
          notCached.add(nextSegment);
        }
      }
    }
    finally {
      // Put non-cached segments back at the front of loadableSegments, which keeps it sorted: they were polled from the
      // front, in order.
      for (int i = notCached.size() - 1; i >= 0; i--) {
        loadableSegments.addFirst(notCached.get(i));
      }
    }
  }

  /**
   * Removes and returns the next segment from {@link #loadableSegments} that {@link #inputPruner} does not rule out,
   * or null if none remain.
   */
  @GuardedBy("this")
  @Nullable
  private QueuedSegment pollUnprunedSegment()
  {
    QueuedSegment nextSegment;
    do {
      nextSegment = loadableSegments.poll();
    } while (nextSegment != null && canSkipSegment(nextSegment.segment()));
    return nextSegment;
  }

  /**
   * Returns whether processing the given segment can be safely skipped.
   */
  private boolean canSkipSegment(final LoadableSegment segment)
  {
    return inputPruner.canSkip(segment.descriptor().getInterval());
  }

  /**
   * Acquires {@code nextSegment}: immediately if it is cached, otherwise by starting to load it. The caller must pass
   * the returned acquisition to {@link #registerReadyCallback} once outside the queue monitor.
   *
   * @param loadahead whether this is a load-ahead acquisition rather than an on-demand one
   */
  @GuardedBy("this")
  private Acquisition acquireSegment(final QueuedSegment nextSegment, final boolean loadahead)
  {
    final ListenableFuture<NumberedReadableInput> cachedFuture = acquireIfCached(nextSegment);
    if (cachedFuture != null) {
      return new Acquisition(nextSegment, cachedFuture, null, loadahead);
    }
    return loadSegment(nextSegment, loadahead);
  }

  /**
   * Acquires {@code nextSegment} if it is cached, returning an already-completed future to it, or null if it is not
   * cached.
   */
  @GuardedBy("this")
  @Nullable
  private ListenableFuture<NumberedReadableInput> acquireIfCached(final QueuedSegment nextSegment)
  {
    final LoadableSegment loadableSegment = nextSegment.segment();
    final Optional<Segment> cachedSegment;
    try {
      cachedSegment = loadableSegment.acquireIfCached(acquireMode);
    }
    catch (Throwable e) {
      if (VirtualStorageManager.isInsufficientStorage(e)) {
        // A locally-cached segment can still need room in the segment cache, e.g. to re-reserve parts of it that were
        // evicted since it was loaded. Treat it as not cached: loading it is subject to the usual rules, so a
        // load-ahead that finds no room is deferred and an on-demand one fails.
        log.debug(
            e,
            "No room in the segment cache to acquire cached segment[%s]; loading it instead.",
            loadableSegment.description()
        );
        return null;
      }
      throw e;
    }
    if (cachedSegment.isEmpty()) {
      return null;
    }
    return Futures.immediateFuture(addLoadedSegment(nextSegment.inputNumber(), loadableSegment, cachedSegment));
  }

  /**
   * Starts loading {@code nextSegment}. The returned acquisition's future is completed by {@link #onSegmentReady}, once
   * {@link #registerReadyCallback} has registered it.
   */
  @GuardedBy("this")
  private Acquisition loadSegment(final QueuedSegment nextSegment, final boolean loadahead)
  {
    final SettableFuture<NumberedReadableInput> future = SettableFuture.create();
    final AcquireSegmentAction acquireSegmentAction = nextSegment.segment().acquire(acquireMode);
    loadingSegments.put(acquireSegmentAction, future);
    return new Acquisition(nextSegment, future, acquireSegmentAction, loadahead);
  }

  /**
   * Wraps a segment acquired for {@code loadableSegment} in a {@link SegmentReferenceHolder}, tracked in
   * {@link #loadedSegments} so {@link #close()} releases it if it is never handed out, and returns the input to hand
   * out. Both ways a segment becomes ready go through here: a cache hit in {@link #acquireIfCached} and a delivered
   * load in {@link #onSegmentReady}.
   */
  @GuardedBy("this")
  private NumberedReadableInput addLoadedSegment(
      final int inputNumber,
      final LoadableSegment loadableSegment,
      final Optional<Segment> segment
  )
  {
    final SegmentReferenceHolder holder = new SegmentReferenceHolder(
        new SegmentReference(loadableSegment.descriptor(), segment),
        loadableSegment.description()
    );
    loadedSegments.add(holder);
    return new NumberedReadableInput(inputNumber, ReadableInput.segment(holder));
  }

  /**
   * Registers {@link #onSegmentReady} on the action of an acquisition that is a load. Must be called outside the queue
   * monitor as the callback fires inline for an already-ready action, and delivery must not run while holding the lock.
   */
  private void registerReadyCallback(final Acquisition acquisition)
  {
    final AcquireSegmentAction action = acquisition.action();
    if (action == null) {
      // A cache hit, which is ready already.
      return;
    }
    // addReadyCallback runs the callback inline for an action that is ready already, and lets anything it throws
    // escape. Track whether it started, so that is not mistaken for a failure to register: onSegmentReady claims the
    // action from loadingSegments before it can throw, which would look just like a load claimed elsewhere.
    final AtomicBoolean callbackStarted = new AtomicBoolean(false);
    try {
      action.addReadyCallback(
          () -> {
            callbackStarted.set(true);
            onSegmentReady(acquisition);
          }
      );
    }
    catch (Throwable e) {
      if (callbackStarted.get()) {
        throw e;
      }
      final boolean claimedElsewhere;
      synchronized (this) {
        // close() clears loadingSegments too, so this covers a load that close() claimed.
        claimedElsewhere = !loadingSegments.containsKey(action);
      }
      if (claimedElsewhere) {
        // close() or releaseSkippedInputs() claimed the action and closed it before we registered, and has failed its
        // future; nothing left to deliver.
        log.debug(e, "Load of segment[%s] was closed before it could be awaited.", acquisition.segment().description());
        return;
      }
      throw e;
    }
  }

  /**
   * Whether no remaining input can contribute, whatever its interval.
   */
  private boolean canSkipRemaining()
  {
    // If all of time can be skipped then no remaining input can contribute, whatever its interval.
    return inputPruner.canSkip(Intervals.ETERNITY);
  }

  /**
   * Delivery path for {@link #loadSegment} which transfers ownership of the acquired segment out of the handle and
   * into a {@link SegmentReferenceHolder}, then completes the future handed to the frame processor. The future is
   * completed OUTSIDE the queue monitor so downstream listeners never run while holding the queue lock; deliveries
   * racing {@link #close()} are arbitrated by a closed re-check plus {@link SettableFuture}'s behavior where the first
   * write wins (close() also fails in-flight futures but the losing write is a harmless no-op).
   * <p>
   * A load-ahead that fails for lack of room in the segment cache before it is handed out is deferred instead of
   * failed; see {@link #deferFailedLoadahead}.
   */
  private void onSegmentReady(final Acquisition acquisition)
  {
    final LoadableSegment loadableSegment = acquisition.segment();
    final AcquireSegmentAction acquireSegmentAction = acquisition.action();
    NumberedReadableInput input = null;
    Throwable failure = null;
    final SettableFuture<NumberedReadableInput> future;

    synchronized (this) {
      future = loadingSegments.remove(acquireSegmentAction);
      if (future == null) {
        // close() already processed this handle, closing it (and any delivered result) and failed the future; nothing
        // left to do.
        return;
      }
      // visible to close() as a mid-delivery future it must also fail
      inFlightDeliveries.add(future);
      // tracks the released result until ownership transfers to a SegmentReferenceHolder in loadedSegments. If a step
      // after release() throws while this is still non-null, we own the orphaned result and must close it (close on
      // the released action is a no-op, so closing the action alone would leak the segment and its folded holds).
      AcquireSegmentResult releasedResult = null;
      try {
        // Ownership transfer; the delivered segment's close releases everything the acquire placed, and release()
        // runs the producer's delivery-accounting hook.
        releasedResult = acquireSegmentAction.release();
        input = addLoadedSegment(acquisition.queued().inputNumber(), loadableSegment, releasedResult.getSegment());
        // ownership is now with loadedSegments; close() will close the holder if it is never handed out
        releasedResult = null;
        if (acquisition.loadahead()) {
          // Load-ahead is working; stop backing off.
          loadaheadBackoff = 0;
        }
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
          boolean deferred;
          try {
            deferred = deferFailedLoadahead(future, acquisition, t);
          }
          catch (Throwable deferralFailure) {
            // Could not defer, so the failure stands and fails the future below like any other failed load. This must
            // not escape else the future would never complete.
            if (deferralFailure != t) {
              t.addSuppressed(deferralFailure);
            }
            deferred = false;
          }
          if (deferred) {
            // The segment is back in loadableSegments; the future was never handed out, so it is simply dropped.
            inFlightDeliveries.remove(future);
            return;
          }
        }
      }
    }

    // Re-check for close (under the monitor) just before completing the future: if the queue closed after the
    // delivery work above, close() has drained (or is draining) the holder we just added to loadedSegments, so fail
    // the future rather than hand the consumer a dead holder. The re-check only narrows that race as close() can still
    // land between reading the flag here and the set() below, leaving the consumer with a holder that close() is
    // also draining. That leftover race is handled by SegmentReferenceHolder.getSegmentReferenceOnce(), whichever of
    // the consumer or close() reaches it first takes the reference and owns closing it, the other gets null, and the
    // segment is never closed twice or leaked.
    final boolean queueClosed;
    synchronized (this) {
      queueClosed = closed;
    }
    if (failure != null) {
      future.setException(failure);
    } else if (queueClosed) {
      future.setException(DruidException.defensive("Input queue closed while segment load was in flight"));
    } else {
      future.set(input);
    }
    synchronized (this) {
      inFlightDeliveries.remove(future);
    }
  }

  /**
   * Defers a load-ahead acquisition whose action failed for lack of room in the segment cache, provided it has not
   * been handed out yet (its future is still in {@link #pendingNextInputs}), so nobody is waiting on it: the
   * acquisition is abandoned, and the segment goes back to be tried again later. Returns whether it was deferred; if
   * not, the failure stands. The caller must have closed the failed action.
   * <p>
   * A load-ahead that was handed out before it failed is needed to make progress, so its failure is never deferred.
   * <p>
   * Throws if the deferral fails, e.g. if {@link LoadableSegment#abandonFailedAcquisition()} does. The future is still
   * in {@link #pendingNextInputs} then, so the caller fails it like any other failed load.
   */
  @GuardedBy("this")
  private boolean deferFailedLoadahead(
      final ListenableFuture<NumberedReadableInput> future,
      final Acquisition acquisition,
      final Throwable failure
  )
  {
    // There is no need to check for close(): the caller claimed this load from loadingSegments in the same monitor
    // hold, and close() clears both loadingSegments and pendingNextInputs.
    if (!acquisition.loadahead()
        || !pendingNextInputs.containsKey(future)
        || !VirtualStorageManager.isInsufficientStorage(failure)) {
      return false;
    }

    // The failed action handed out no segment, so the segment can be made acquirable again.
    acquisition.segment().abandonFailedAcquisition();

    // Remove the future last, so that it is still pending if anything before this throws.
    deferLoadahead(acquisition.queued(), failure);
    removePendingInput(future);
    return true;
  }

  /**
   * Puts the segment of a load-ahead acquisition that found no room in the segment cache back into
   * {@link #loadableSegments}, and backs off load-ahead for a while unless a {@link #loadaheadCooldown} is running
   * already (see {@link #loadaheadBackoff}).
   * <p>
   * Load-ahead is purely a prefetch, so a full cache must not fail the query. The segment is tried again on a later
   * load-ahead opportunity, or acquired on demand by {@link #nextSegmentInput()} once nothing is pending. It goes back
   * to its own place in hand-out order (see {@link #requeue}) rather than onto the front: several load-aheads can be in
   * flight when the cache fills up, and their failures can arrive in any order, so pushing each onto the front would
   * reverse them. The count reported by {@link #remaining()} is unaffected, since the segment merely moves back to
   * where it came from.
   */
  @GuardedBy("this")
  private void deferLoadahead(final QueuedSegment segment, final Throwable e)
  {
    requeue(segment);
    if (loadaheadCooldown == 0) {
      loadaheadBackoff = Math.min(MAX_LOADAHEAD_COOLDOWN, loadaheadBackoff == 0 ? 1 : loadaheadBackoff * 2);
      loadaheadCooldown = loadaheadBackoff;
    }

    if (loggedLoadaheadDeferral) {
      log.debug(
          e,
          "No room in the segment cache to load segment[%s] ahead; backing off load-ahead for [%d] input(s).",
          segment.segment().description(),
          loadaheadCooldown
      );
    } else {
      loggedLoadaheadDeferral = true;
      log.noStackTrace().info(
          e,
          "No room in the segment cache to load segment[%s] ahead; backing off load-ahead for [%d] input(s). "
          + "Segments are still loaded on demand as they are needed, so this only means load-ahead is backing off.",
          segment.segment().description(),
          loadaheadCooldown
      );
    }
  }

  /**
   * Puts a segment that was taken from {@link #loadableSegments} back in its place, keeping the queue sorted by
   * {@link QueuedSegment#position()}. Segments are always taken from the front, so only segments deferred earlier can
   * come before it, and there are few of those: load-ahead keeps at most {@link #loadahead} acquisitions in flight.
   */
  @GuardedBy("this")
  private void requeue(final QueuedSegment segment)
  {
    final List<QueuedSegment> before = new ArrayList<>();
    while (!loadableSegments.isEmpty() && loadableSegments.peekFirst().position() < segment.position()) {
      before.add(loadableSegments.pollFirst());
    }
    loadableSegments.addFirst(segment);
    for (int i = before.size() - 1; i >= 0; i--) {
      loadableSegments.addFirst(before.get(i));
    }
  }

  /**
   * Acquires the next segment that {@link #inputPruner} does not rule out and adds its future to
   * {@link #pendingNextInputs}. Returns the acquisition, for the caller to pass to {@link #registerReadyCallback} once
   * outside the queue monitor, or null if no segment was acquired: either none remain, or the segment cache had no
   * room for the acquisition, in which case {@link #deferLoadahead} has put the segment back.
   */
  @GuardedBy("this")
  @Nullable
  private Acquisition addLoadaheadFuture()
  {
    if (canSkipRemaining()) {
      return null;
    }

    final QueuedSegment nextSegment = pollUnprunedSegment();
    if (nextSegment == null) {
      return null;
    }

    final Acquisition acquisition;
    try {
      acquisition = acquireSegment(nextSegment, true);
    }
    catch (Throwable e) {
      if (VirtualStorageManager.isInsufficientStorage(e)) {
        // acquire() leaves the segment unacquired when it throws, so it can go straight back into the queue.
        deferLoadahead(nextSegment, e);
        return null;
      }
      throw e;
    }
    pendingNextInputs.put(acquisition.future(), new PendingInput(nextSegment.segment(), false));
    pendingLoadaheadCount++;
    return acquisition;
  }

  /**
   * Removes a future from {@link #pendingNextInputs}, keeping {@link #pendingLoadaheadCount} in step, and returns the
   * input it delivers.
   */
  @GuardedBy("this")
  private PendingInput removePendingInput(final ListenableFuture<NumberedReadableInput> future)
  {
    final PendingInput input = pendingNextInputs.remove(future);
    if (!input.cached()) {
      pendingLoadaheadCount--;
    }
    return input;
  }

  /**
   * Releases pending inputs that will never be handed out for processing. Loads still in progress are canceled, and
   * loaded inputs are released. An input that is mid-delivery (claimed from {@link #loadingSegments} by
   * {@link #onSegmentReady} but not yet completed) is left for {@link #close()}; this is a conscious decision to
   * avoid complexity for what is a sliver of a race.
   * <p>
   * Called outside the queue monitor, because canceling a load can run its canceler and releasing a segment can take
   * cache locks.
   */
  private void releaseSkippedInputs(final List<ListenableFuture<NumberedReadableInput>> skippedFutures)
  {
    if (skippedFutures.isEmpty()) {
      return;
    }

    final Set<ListenableFuture<NumberedReadableInput>> pendingSkips = Sets.newIdentityHashSet();
    pendingSkips.addAll(skippedFutures);
    final Map<AcquireSegmentAction, SettableFuture<NumberedReadableInput>> loadsToCancel = new LinkedHashMap<>();

    synchronized (this) {
      final Iterator<Map.Entry<AcquireSegmentAction, SettableFuture<NumberedReadableInput>>> iterator =
          loadingSegments.entrySet().iterator();
      while (iterator.hasNext()) {
        final Map.Entry<AcquireSegmentAction, SettableFuture<NumberedReadableInput>> entry = iterator.next();
        if (pendingSkips.remove(entry.getValue())) {
          loadsToCancel.put(entry.getKey(), entry.getValue());
          iterator.remove();
        }
      }
    }

    // Cancel all pending segment loads in segments that have been skipped.
    for (final Map.Entry<AcquireSegmentAction, SettableFuture<NumberedReadableInput>> entry : loadsToCancel.entrySet()) {
      closeLoadingSegment(entry.getKey());
      entry.getValue().setException(DruidException.defensive("Segment load canceled because its input was skipped"));
    }

    // Lastly, release completed loads that were skipped if their future is done.
    for (final ListenableFuture<NumberedReadableInput> future : pendingSkips) {
      if (future.isDone()) {
        final NumberedReadableInput input;
        try {
          input = Futures.getDone(future);
        }
        catch (ExecutionException | CancellationException e) {
          // A failed load has already closed its handle in onSegmentReady, so there is nothing to release.
          continue;
        }
        releaseLoadedSegment(input.input().getSegment());
      }
    }
  }

  /**
   * Removes {@code holder} from {@link #loadedSegments} and closes its segment, unless {@link #close()} claimed it
   * first.
   */
  private void releaseLoadedSegment(final SegmentReferenceHolder holder)
  {
    synchronized (this) {
      loadedSegments.remove(holder);
    }
    closeLoadedSegment(holder);
  }

  /**
   * Closes a load that has not been delivered: closing a NEW handle runs its canceler (aborting the load), and closing
   * a ready but unclaimed handle (its ready callback hasn't reached the queue monitor yet) closes the delivered result.
   * The caller must have claimed the handle's entry from {@link #loadingSegments}, and is responsible for failing its
   * future. Called outside the queue monitor.
   */
  private static void closeLoadingSegment(final AcquireSegmentAction acquireSegmentAction)
  {
    CloseableUtils.closeAndSuppressExceptions(
        acquireSegmentAction,
        // AcquireSegmentAction currently doesn't have a meaningful toString method, so if this message
        // ever actually gets logged, it won't mention the specific segment that had a problem. Perhaps
        // one day this will change.
        e -> log.warn(e, "Failed to close loadingSegment[%s]", acquireSegmentAction)
    );
  }

  /**
   * Claims the segment reference from {@code holder} and closes it. Does nothing if the reference was already claimed,
   * by a caller it was handed out to or by another release path, so the segment is closed exactly once. Called outside
   * the queue monitor.
   */
  private static void closeLoadedSegment(final SegmentReferenceHolder holder)
  {
    final SegmentReference ref = holder.getSegmentReferenceOnce();
    if (ref != null) {
      CloseableUtils.closeAndSuppressExceptions(
          ref,
          e -> log.warn(e, "Failed to close loadedSegment[%s]", ref.getSegmentDescriptor())
      );
    }
  }

  /**
   * Returns the comparator that sorts intervals into {@code timeOrder}, so intervals that can hold the best rows come
   * first. Intervals compare equal when they bound their best possible row at the same time. With {@link Order#NONE},
   * every pair compares equal, so {@link #selectPendingInput()} may hand out any loaded input first.
   */
  private static Comparator<Interval> makeIntervalOrder(final Order timeOrder)
  {
    return switch (timeOrder) {
      case ASCENDING -> Comparator.comparingLong(Interval::getStartMillis);
      case DESCENDING -> Comparator.comparingLong(Interval::getEndMillis).reversed();
      case NONE -> (_, _) -> 0;
    };
  }

  @Override
  public void close()
  {
    final List<AcquireSegmentAction> handlesToClose;
    final List<SegmentReferenceHolder> holdersToDrain;
    final List<SettableFuture<NumberedReadableInput>> futuresToFail;

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
      futuresToFail = new ArrayList<>(loadingSegments.values());
      loadingSegments.clear();

      // Also fail deliveries that are mid-completion (removed from loadingSegments but not yet completed)
      futuresToFail.addAll(inFlightDeliveries);

      holdersToDrain = new ArrayList<>(loadedSegments);
      loadedSegments.clear();

      // Drop loadahead futures that were never handed out: their loads are covered by the closing/failing here, and
      // handing them out after close would deliver holders this close() is draining. Also keeps remaining()
      // reporting 0 after close.
      pendingNextInputs.clear();
      pendingLoadaheadCount = 0;
    }

    // Cancel all pending segment loads.
    for (final AcquireSegmentAction acquireSegmentAction : handlesToClose) {
      closeLoadingSegment(acquireSegmentAction);
    }

    // Close all segments that have been loaded and not yet transferred to callers. (Segments transferred to
    // callers must be closed by the callers.)
    for (final SegmentReferenceHolder referenceHolder : holdersToDrain) {
      closeLoadedSegment(referenceHolder);
    }

    // Explicitly fail the futures of the loads we just canceled. Closing a handle fires its pending ready callback,
    // but that callback finds its entry already claimed from loadingSegments above and returns without completing the
    // future, so without this the frame processors awaiting these futures would hang forever.
    for (final SettableFuture<NumberedReadableInput> future : futuresToFail) {
      future.setException(DruidException.defensive("Input queue closed while segment load was in flight"));
    }
  }

  /**
   * A {@link ReadableInput} along with the input number it came from.
   */
  public record NumberedReadableInput(int inputNumber, ReadableInput input)
  {
  }

  /**
   * A segment in {@link #pendingNextInputs}.
   * <p>
   * Cached segments come from {@link #acquireCachedInNextGroup()} and are marked accordingly so consumers are aware
   * of how the segment was acquired. The others were acquired by load-ahead, and count toward {@link #loadahead}.
   */
  private record PendingInput(LoadableSegment segment, boolean cached)
  {
    Interval interval()
    {
      return segment.descriptor().getInterval();
    }
  }

  /**
   * A segment acquisition started under the queue monitor by {@link #acquireSegment}. Unless it was a cache hit, it has
   * an {@link #action} whose ready callback {@link #registerReadyCallback} registers once outside the monitor.
   *
   * @param queued    the segment, as it was taken from {@link #loadableSegments}
   * @param future    future to hand out for the segment
   * @param action    the load, or null for a cache hit
   * @param loadahead whether this is a load-ahead acquisition rather than an on-demand one
   */
  private record Acquisition(
      QueuedSegment queued,
      ListenableFuture<NumberedReadableInput> future,
      @Nullable AcquireSegmentAction action,
      boolean loadahead
  )
  {
    LoadableSegment segment()
    {
      return queued.segment();
    }
  }

  /**
   * An item that is yet to be turned into a {@link ReadableInput}, along with the input number it came from.
   */
  private record NumberedItem<T>(int inputNumber, T item)
  {
  }

  /**
   * A segment in {@link #loadableSegments}, along with the input number it came from.
   *
   * @param position position of the segment in hand-out order: its index in {@link #loadableSegments} as built by the
   *                 constructor. Lets {@link #requeue} put a segment back in its place.
   */
  private record QueuedSegment(int position, int inputNumber, LoadableSegment segment)
  {
  }
}
