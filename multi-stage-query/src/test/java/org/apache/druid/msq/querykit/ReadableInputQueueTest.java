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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.query.Order;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Tests for {@link ReadableInputQueue}'s segment-load lifecycle: close-while-loading must promptly fail the futures
 * handed to frame processors (the queue claims each pending load before closing its handle, so the close-fired ready
 * callback finds nothing to deliver and the queue's explicit failure is what completes the future), delivered
 * segments transfer exactly once, and everything un-transferred is closed by the queue.
 */
class ReadableInputQueueTest
{
  private static final SegmentDescriptor DESCRIPTOR = SegmentId.dummy("test").toDescriptor();

  private static class CountingSegment implements Segment
  {
    final AtomicInteger closes = new AtomicInteger();
    @Nullable
    private final Runnable onClose;

    CountingSegment()
    {
      this(null);
    }

    CountingSegment(@Nullable Runnable onClose)
    {
      this.onClose = onClose;
    }

    @Override
    public SegmentId getId()
    {
      return SegmentId.dummy("test");
    }

    @Override
    public Interval getDataInterval()
    {
      return Intervals.ETERNITY;
    }

    @Nullable
    @Override
    public <T> T as(@Nonnull Class<T> clazz)
    {
      return null;
    }

    @Override
    public void close()
    {
      if (closes.incrementAndGet() == 1 && onClose != null) {
        onClose.run();
      }
    }
  }

  private static class TestLoadableSegment implements LoadableSegment
  {
    private final AcquireSegmentAction action;
    private final Optional<Segment> cached;
    private final boolean throwOnDescriptor;
    private final SegmentDescriptor descriptor;
    final AtomicInteger countDeliveredCalls = new AtomicInteger();
    final AtomicInteger countAcquires = new AtomicInteger();
    final AtomicInteger countHandedOutCalls = new AtomicInteger();
    final AtomicInteger abandonCalls = new AtomicInteger();

    TestLoadableSegment(AcquireSegmentAction action)
    {
      this(action, Optional.empty(), false);
    }

    TestLoadableSegment(AcquireSegmentAction action, Optional<Segment> cached)
    {
      this(action, cached, false);
    }

    TestLoadableSegment(AcquireSegmentAction action, Optional<Segment> cached, boolean throwOnDescriptor)
    {
      this(action, cached, throwOnDescriptor, DESCRIPTOR);
    }

    TestLoadableSegment(
        AcquireSegmentAction action,
        Optional<Segment> cached,
        boolean throwOnDescriptor,
        SegmentDescriptor descriptor
    )
    {
      this.action = action;
      this.cached = cached;
      this.throwOnDescriptor = throwOnDescriptor;
      this.descriptor = descriptor;
    }

    @Override
    public SegmentDescriptor descriptor()
    {
      // Simulates a post-release failure in the queue's delivery path: onSegmentReady calls descriptor() after
      // release() has already transferred ownership of the result. Calls before then, such as for pruning, succeed.
      if (throwOnDescriptor && countDeliveredCalls.get() > 0) {
        throw new IllegalStateException("descriptor blew up");
      }
      return descriptor;
    }

    @Nullable
    @Override
    public String description()
    {
      return "test-loadable";
    }

    @Override
    public Optional<Segment> acquireIfCached(AcquireMode acquireMode)
    {
      if (cached.isPresent()) {
        countAcquires.incrementAndGet();
      }
      return cached;
    }

    @Override
    public AcquireSegmentAction acquire(AcquireMode acquireMode)
    {
      // install delivery accounting the same way real implementations do: a successful release counts exactly once
      countAcquires.incrementAndGet();
      action.setOnRelease(result -> countDeliveredCalls.incrementAndGet());
      return action;
    }

    @Override
    public void abandonFailedAcquisition()
    {
      abandonCalls.incrementAndGet();
    }

    @Override
    public void countHandedOut()
    {
      countHandedOutCalls.incrementAndGet();
    }

    @Override
    public ListenableFuture<DataSegment> dataSegmentFuture()
    {
      return Futures.immediateFailedFuture(DruidException.defensive("not available"));
    }
  }

  private static List<Integer> acquireCounts(final List<CacheBackedLoadableSegment> segments)
  {
    final List<Integer> counts = new ArrayList<>();
    for (final CacheBackedLoadableSegment segment : segments) {
      counts.add(segment.acquireCount.get());
    }
    return counts;
  }

  private static List<Integer> abandonCounts(final List<CacheBackedLoadableSegment> segments)
  {
    final List<Integer> counts = new ArrayList<>();
    for (final CacheBackedLoadableSegment segment : segments) {
      counts.add(segment.abandonCount.get());
    }
    return counts;
  }

  /**
   * Completes every manual-mode acquisition that is waiting, in list order.
   */
  private static void completePendingLoads(final List<CacheBackedLoadableSegment> segments)
  {
    for (final CacheBackedLoadableSegment segment : segments) {
      if (segment.hasPendingLoad()) {
        segment.completeLoad();
      }
    }
  }

  /**
   * Hands out the next input of a queue over manual-mode segments and returns its descriptor, closing its reference.
   * Completes every waiting acquisition before asking for the input, so the queue picks among ready inputs in its own
   * order, and again afterwards, for one it acquires on demand. The cache must have room for them all.
   */
  private static SegmentDescriptor takeCompletingLoads(
      final ReadableInputQueue queue,
      final List<CacheBackedLoadableSegment> segments
  ) throws Exception
  {
    completePendingLoads(segments);
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    completePendingLoads(segments);
    final SegmentReference ref = future.get(10, TimeUnit.SECONDS).input().getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    final SegmentDescriptor descriptor = ref.getSegmentDescriptor();
    ref.close();
    return descriptor;
  }

  private static void closeQuietly(@Nullable final Closeable closeable)
  {
    if (closeable != null) {
      try {
        closeable.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static List<CacheBackedLoadableSegment> makeSegments(
      final int count,
      final TestSegmentCache cache,
      final NoRoomStyle noRoomStyle
  )
  {
    final List<CacheBackedLoadableSegment> segments = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      segments.add(new CacheBackedLoadableSegment(i, cache, noRoomStyle, false));
    }
    return segments;
  }

  private static Set<SegmentDescriptor> descriptorsOf(final List<? extends LoadableSegment> segments)
  {
    final Set<SegmentDescriptor> descriptors = new HashSet<>();
    for (final LoadableSegment segment : segments) {
      descriptors.add(segment.descriptor());
    }
    return descriptors;
  }

  /**
   * Hands out every remaining input, closing each segment reference before asking for the next one, and returns the
   * descriptors handed out, in order.
   */
  private static List<SegmentDescriptor> drainInOrder(final ReadableInputQueue queue) throws Exception
  {
    final List<SegmentDescriptor> descriptors = new ArrayList<>();
    ListenableFuture<ReadableInputQueue.NumberedReadableInput> future;
    while ((future = queue.nextInput()) != null) {
      final SegmentReference ref = future.get(10, TimeUnit.SECONDS).input().getSegment().getSegmentReferenceOnce();
      Assertions.assertNotNull(ref);
      descriptors.add(ref.getSegmentDescriptor());
      ref.close();
    }
    return descriptors;
  }

  /**
   * Like {@link #drainInOrder}, but returns a set, checking that no input was handed out twice.
   */
  private static Set<SegmentDescriptor> drainDescriptors(final ReadableInputQueue queue) throws Exception
  {
    final List<SegmentDescriptor> handedOut = drainInOrder(queue);
    final Set<SegmentDescriptor> descriptors = new HashSet<>(handedOut);
    Assertions.assertEquals(handedOut.size(), descriptors.size(), "handed out twice: " + handedOut);
    return descriptors;
  }

  /**
   * Hands out the next input and returns its segment reference, which the caller closes.
   */
  private static SegmentReference take(final ReadableInputQueue queue) throws Exception
  {
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final SegmentReference ref = future.get(10, TimeUnit.SECONDS).input().getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    return ref;
  }

  private static ReadableInputQueue makeQueue(int loadahead, List<? extends LoadableSegment> segments)
  {
    return makeQueue(loadahead, InputPruner.INCLUDE_ALL, segments.toArray(new LoadableSegment[0]));
  }

  private static ReadableInputQueue makeQueue(int loadahead, LoadableSegment... segments)
  {
    return makeQueue(loadahead, InputPruner.INCLUDE_ALL, segments);
  }

  private static ReadableInputQueue makeQueue(
      int loadahead,
      InputPruner inputPruner,
      LoadableSegment... segments
  )
  {
    return new ReadableInputQueue(
        null,
        IntList.of(0),
        List.of(new PhysicalInputSlice(ReadablePartitions.empty(), List.of(segments), List.of())),
        loadahead,
        AcquireMode.FULL,
        inputPruner
    );
  }

  @Test
  void testCloseWhileLoadingFailsPendingFutureWithoutHanging() throws Exception
  {
    final AtomicInteger cancels = new AtomicInteger();
    final AcquireSegmentAction pending = new AcquireSegmentAction(cancels::incrementAndGet);
    final TestLoadableSegment loadable = new TestLoadableSegment(pending);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertFalse(future.isDone());

    queue.close();

    // the future must fail promptly; a dropped ready-callback with no explicit failure would hang forever
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("Input queue closed"), e.getCause().getMessage());
    Assertions.assertEquals(1, cancels.get(), "canceler must run exactly once");
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "cancelled loads must not be counted");
    Assertions.assertEquals(0, loadable.countHandedOutCalls.get(), "failed inputs must not be counted as read");
  }

  @Test
  void testCloseWithReentrantDeliveringCancelerFailsAllFutures() throws Exception
  {
    // Reproduces the deferred-acquire close race: closing a NEW handle runs a canceler that synchronously completes
    // the SAME handle (mimicking dsFuture.cancel(true) -> onFailure -> outer.setException). With multiple such handles
    // in flight, close() must not throw ConcurrentModificationException from a reentrant onSegmentReady, and must fail
    // every pending future rather than leaving frame processors hung.
    final int count = 4;
    final List<ListenableFuture<ReadableInputQueue.NumberedReadableInput>> futures = new ArrayList<>();
    final TestLoadableSegment[] loadables = new TestLoadableSegment[count];
    for (int i = 0; i < count; i++) {
      final AcquireSegmentAction[] holder = new AcquireSegmentAction[1];
      // canceler completes the handle in place, as the real deferred canceler does via the coordinator future
      final AcquireSegmentAction action = new AcquireSegmentAction(
          () -> holder[0].setException(DruidException.defensive("canceled"))
      );
      holder[0] = action;
      loadables[i] = new TestLoadableSegment(action);
    }

    final ReadableInputQueue queue = makeQueue(0, loadables);
    queue.start();
    for (int i = 0; i < count; i++) {
      final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
      Assertions.assertNotNull(future);
      Assertions.assertFalse(future.isDone());
      futures.add(future);
    }

    // Must not throw ConcurrentModificationException.
    queue.close();

    for (final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future : futures) {
      final ExecutionException e = Assertions.assertThrows(
          ExecutionException.class,
          () -> future.get(10, TimeUnit.SECONDS)
      );
      Assertions.assertTrue(e.getCause().getMessage().contains("Input queue closed"), e.getCause().getMessage());
    }
    for (final TestLoadableSegment loadable : loadables) {
      Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "canceled loads must not be counted");
    }
  }

  @Test
  void testPostReleaseFailureClosesResultInsteadOfLeaking() throws Exception
  {
    // release() succeeds, then a later delivery step (descriptor()) throws: the released result must be closed, not
    // leaked (closing the RELEASED action is a no-op, so the segment + its folded holds would otherwise leak forever).
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed, Optional.empty(), true);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);

    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("descriptor blew up"), e.getCause().getMessage());
    Assertions.assertEquals(1, segment.closes.get(), "post-release failure must close the delivered segment");
    // release succeeded, so the load was counted, but the failed input was never read
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get());
    Assertions.assertEquals(0, loadable.countHandedOutCalls.get());

    // nothing left to close on queue teardown
    queue.close();
    Assertions.assertEquals(1, segment.closes.get(), "segment must not be double-closed");
  }

  @Test
  void testProducerOwnsResultDeliveredAfterQueueClose()
  {
    final AcquireSegmentAction pending = new AcquireSegmentAction();
    final TestLoadableSegment loadable = new TestLoadableSegment(pending);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    queue.close();

    // the producer's delivery loses the race with close: set() returns false and the producer closes the orphan
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentResult result = AcquireSegmentResult.of(Optional.of(segment));
    Assertions.assertFalse(pending.set(result));
    result.close();
    Assertions.assertEquals(1, segment.closes.get());
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get());
    Assertions.assertEquals(0, loadable.countHandedOutCalls.get());
  }

  @Test
  void testReadyButUncollectedSegmentClosedOnQueueClose()
  {
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed);

    // loadahead=1: start() drives the load and the immediate-fire ready callback delivers into loadedSegments
    final ReadableInputQueue queue = makeQueue(1, loadable);
    queue.start();
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get(), "delivery must be counted exactly once");

    // never call nextInput(): the loaded-but-untransferred segment must be closed by the queue
    queue.close();
    Assertions.assertEquals(1, segment.closes.get());
    Assertions.assertEquals(0, loadable.countHandedOutCalls.get(), "inputs never handed out must not be counted");
  }

  @Test
  void testTransferredReferenceIsCallersResponsibility() throws Exception
  {
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final ReadableInput input = future.get(10, TimeUnit.SECONDS).input();
    Assertions.assertTrue(input.hasSegment());

    final SegmentReference ref = input.getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get());
    Assertions.assertEquals(1, loadable.countHandedOutCalls.get());

    // queue close must not close a transferred reference
    queue.close();
    Assertions.assertEquals(0, segment.closes.get());

    ref.close();
    Assertions.assertEquals(1, segment.closes.get());
  }

  @Test
  void testLoadFailurePropagatesToFuture()
  {
    final AcquireSegmentAction failed = new AcquireSegmentAction();
    failed.setException(new IllegalStateException("load went sideways"));
    final TestLoadableSegment loadable = new TestLoadableSegment(failed);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertTrue(future.isDone());
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("load went sideways"));
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "failed loads must not be counted");
    Assertions.assertEquals(0, loadable.countHandedOutCalls.get(), "failed loads must not be counted as read");
    queue.close();
  }

  @Test
  void testMissingSegmentDelivery() throws Exception
  {
    final TestLoadableSegment loadable = new TestLoadableSegment(AcquireSegmentAction.missingSegment());

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final ReadableInput input = future.get(10, TimeUnit.SECONDS).input();
    Assertions.assertTrue(input.hasSegment());
    final SegmentReference ref = input.getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    Assertions.assertTrue(ref.getSegmentReference().isEmpty());
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get());
    Assertions.assertEquals(1, loadable.countHandedOutCalls.get());
    ref.close();
    queue.close();
  }

  @Test
  void testCachedAtStartPathBypassesAcquire()
      throws Exception
  {
    final CountingSegment cachedSegment = new CountingSegment();
    // the action would throw if released; the cached path must never touch it
    final TestLoadableSegment loadable = new TestLoadableSegment(
        new AcquireSegmentAction(),
        Optional.of(cachedSegment)
    );

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertTrue(future.isDone());
    final ReadableInput input = future.get(10, TimeUnit.SECONDS).input();
    final SegmentReference ref = input.getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    Assertions.assertSame(cachedSegment, ref.getSegmentReference().orElseThrow());
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "a cached segment has no delivery to count");
    Assertions.assertEquals(1, loadable.countHandedOutCalls.get());
    ref.close();
    queue.close();
  }

  @Test
  void testCloseClearsPendingLoadaheadInputs() throws Exception
  {
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed);

    // loadahead=1: start() loads and parks a done future in pendingNextInputs that is never handed out
    final ReadableInputQueue queue = makeQueue(1, loadable);
    queue.start();
    Assertions.assertEquals(1, queue.remaining());

    queue.close();
    Assertions.assertEquals(1, segment.closes.get(), "unclaimed loadahead holder must be drained on close");
    Assertions.assertEquals(0, queue.remaining(), "close must clear pending loadahead inputs");
    Assertions.assertNull(queue.nextInput(), "no inputs may be handed out after close");
  }

  @Test
  void testCanSkipRemainingStopsHandingOutInputs()
  {
    final CountingSegment cachedSegment = new CountingSegment();
    final TestLoadableSegment loadable = new TestLoadableSegment(new AcquireSegmentAction(), Optional.of(cachedSegment));
    final AtomicBoolean canSkipRemaining = new AtomicBoolean(false);

    final ReadableInputQueue queue = makeQueue(0, interval -> canSkipRemaining.get(), loadable);
    queue.start();
    Assertions.assertEquals(1, queue.remaining());

    canSkipRemaining.set(true);
    Assertions.assertNull(queue.nextInput(), "no inputs may be handed out once remaining inputs can be skipped");
    Assertions.assertEquals(1, cachedSegment.closes.get(), "skipped cached segment must be released before close");
    Assertions.assertEquals(0, loadable.countHandedOutCalls.get(), "skipped inputs must not be counted as read");

    queue.close();
    Assertions.assertEquals(1, cachedSegment.closes.get(), "skipped cached segment must not be closed twice");
  }

  @Test
  void testCanSkipRemainingCancelsPendingLoadsBeforeClose()
  {
    final CountingSegment cachedSegment = new CountingSegment();
    final TestLoadableSegment cached = new TestLoadableSegment(new AcquireSegmentAction(), Optional.of(cachedSegment));
    final AtomicInteger cancels = new AtomicInteger();
    final TestLoadableSegment loading = new TestLoadableSegment(new AcquireSegmentAction(cancels::incrementAndGet));
    final AtomicBoolean canSkipRemaining = new AtomicBoolean(false);

    // loadahead=1: start() parks the cached segment, then starts loading the other one.
    final ReadableInputQueue queue = makeQueue(1, interval -> canSkipRemaining.get(), cached, loading);
    queue.start();
    Assertions.assertEquals(1, loading.countAcquires.get());

    canSkipRemaining.set(true);
    Assertions.assertNull(queue.nextInput());
    Assertions.assertEquals(1, cancels.get(), "skipped load must be canceled before close");
    Assertions.assertEquals(1, cachedSegment.closes.get(), "skipped cached segment must be released before close");
    Assertions.assertEquals(0, queue.remaining());

    queue.close();
    Assertions.assertEquals(1, cachedSegment.closes.get(), "skipped cached segment must not be closed twice");
    Assertions.assertEquals(1, cancels.get(), "skipped load must not be canceled twice");
    Assertions.assertEquals(0, loading.countDeliveredCalls.get(), "canceled loads must not be counted");
  }

  @Test
  void testTimeOrderSkippedLoadedSegmentReleasedBeforeClose()
  {
    final CountingSegment day2Segment = new CountingSegment();
    final TestLoadableSegment day2 = new TestLoadableSegment(
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(day2Segment))),
        Optional.empty(),
        false,
        descriptorOnDay(2, 0)
    );
    final AtomicBoolean skipDay2 = new AtomicBoolean(false);

    // loadahead=2: start() loads both segments before day 2 becomes skippable.
    final ReadableInputQueue queue = makeQueue(
        2,
        timeOrderedPruner(Order.DESCENDING, interval -> skipDay2.get() && interval.equals(day(2))),
        cachedSegmentOnDay(3),
        day2
    );
    queue.start();
    Assertions.assertEquals(1, day2.countDeliveredCalls.get(), "day 2 must be loaded before it becomes skippable");

    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> day3Future = queue.nextInput();
    Assertions.assertEquals(day(3), Futures.getUnchecked(day3Future).input().getSegment().getDescriptor().getInterval());

    skipDay2.set(true);
    Assertions.assertNull(queue.nextInput());
    Assertions.assertEquals(1, day2Segment.closes.get(), "skipped loaded segment must be released before close");
    Assertions.assertEquals(0, day2.countHandedOutCalls.get(), "skipped loaded segment must not be counted as read");

    closeHandedOut(day3Future);
    queue.close();
    Assertions.assertEquals(1, day2Segment.closes.get(), "skipped loaded segment must not be closed twice");
  }

  @Test
  void testTimeOrderSkippedLoadCanceledBeforeClose()
  {
    final AtomicInteger cancels = new AtomicInteger();
    final AcquireSegmentAction day2Action = new AcquireSegmentAction(cancels::incrementAndGet);
    final TestLoadableSegment day2 =
        new TestLoadableSegment(day2Action, Optional.empty(), false, descriptorOnDay(2, 0));
    final AtomicBoolean skipDay2 = new AtomicBoolean(false);

    // loadahead=2: start() starts loading day 2 before it becomes skippable.
    final ReadableInputQueue queue = makeQueue(
        2,
        timeOrderedPruner(Order.DESCENDING, interval -> skipDay2.get() && interval.equals(day(2))),
        cachedSegmentOnDay(3),
        day2
    );
    queue.start();
    Assertions.assertEquals(1, day2.countAcquires.get());

    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> day3Future = queue.nextInput();
    Assertions.assertEquals(day(3), Futures.getUnchecked(day3Future).input().getSegment().getDescriptor().getInterval());

    skipDay2.set(true);
    Assertions.assertNull(queue.nextInput());
    Assertions.assertEquals(1, cancels.get(), "skipped load must be canceled before close");

    // A delivery arriving after the cancel loses, so the producer keeps ownership of its result.
    final CountingSegment lateSegment = new CountingSegment();
    final AcquireSegmentResult lateResult = AcquireSegmentResult.of(Optional.of(lateSegment));
    Assertions.assertFalse(day2Action.set(lateResult));
    lateResult.close();

    closeHandedOut(day3Future);
    queue.close();
    Assertions.assertEquals(1, cancels.get(), "skipped load must not be canceled twice");
    Assertions.assertEquals(1, lateSegment.closes.get());
    Assertions.assertEquals(0, day2.countDeliveredCalls.get(), "canceled loads must not be counted");
  }

  @Test
  void testTimeOrderSkippedFailedLoadHasNothingToRelease()
  {
    final AtomicInteger cancels = new AtomicInteger();
    final AcquireSegmentAction day2Action = new AcquireSegmentAction(cancels::incrementAndGet);
    final TestLoadableSegment day2 =
        new TestLoadableSegment(day2Action, Optional.empty(), false, descriptorOnDay(2, 0));
    final AtomicBoolean skipDay2 = new AtomicBoolean(false);

    final ReadableInputQueue queue = makeQueue(
        2,
        timeOrderedPruner(Order.DESCENDING, interval -> skipDay2.get() && interval.equals(day(2))),
        cachedSegmentOnDay(3),
        day2
    );
    queue.start();

    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> day3Future = queue.nextInput();
    Assertions.assertEquals(day(3), Futures.getUnchecked(day3Future).input().getSegment().getDescriptor().getInterval());

    // The load fails before day 2 becomes skippable, so its pending future is already failed when it is skipped.
    day2Action.setException(DruidException.defensive("load failed"));

    skipDay2.set(true);
    Assertions.assertNull(queue.nextInput());
    Assertions.assertEquals(0, cancels.get(), "a failed load must not be canceled");
    Assertions.assertEquals(0, day2.countDeliveredCalls.get(), "a failed load must not be counted");

    closeHandedOut(day3Future);
    queue.close();
    Assertions.assertEquals(0, cancels.get());
  }

  @Test
  void testCanSkipRemainingStopsLoadahead()
  {
    final AtomicInteger cancels = new AtomicInteger();
    final TestLoadableSegment loadable = new TestLoadableSegment(new AcquireSegmentAction(cancels::incrementAndGet));

    final ReadableInputQueue queue = makeQueue(1, interval -> true, loadable);
    queue.start();
    Assertions.assertNull(queue.nextInput());

    queue.close();
    Assertions.assertEquals(0, cancels.get(), "loadahead must not start a load once remaining inputs can be skipped");
  }

  @Test
  void testRemainingCountsPendingInputs()
  {
    final TestLoadableSegment loadable = new TestLoadableSegment(AcquireSegmentAction.missingSegment());
    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    Assertions.assertEquals(1, queue.remaining());
    Assertions.assertNotNull(queue.nextInput());
    Assertions.assertEquals(0, queue.remaining());
    Assertions.assertNull(queue.nextInput());
    queue.close();
  }

  @ParameterizedTest
  @EnumSource(value = Order.class, names = {"ASCENDING", "DESCENDING"})
  void testTimeOrderHandsOutSegmentsInTimeOrder(final Order order)
  {
    final TestLoadableSegment day1 = cachedSegmentOnDay(1);
    final TestLoadableSegment day2 = cachedSegmentOnDay(2);
    final TestLoadableSegment day3 = cachedSegmentOnDay(3);
    final ReadableInputQueue queue = makeQueue(0, timeOrderedPruner(order, interval -> false), day2, day1, day3);
    queue.start();

    // Only the first interval in time order is acquired up front; later ones are acquired as they are reached.
    Assertions.assertEquals(1, day1.countAcquires.get() + day3.countAcquires.get());
    Assertions.assertEquals(0, day2.countAcquires.get());

    final List<Interval> expected = order == Order.DESCENDING ? List.of(day(3), day(2), day(1))
                                                              : List.of(day(1), day(2), day(3));
    Assertions.assertEquals(expected, handOutAll(queue));
    queue.close();
  }

  @Test
  void testTimeOrderDoesNotAcquirePrunedSegments()
  {
    final TestLoadableSegment pruned = cachedSegmentOnDay(1);
    final ReadableInputQueue queue = makeQueue(
        1,
        timeOrderedPruner(Order.DESCENDING, interval -> interval.equals(day(1))),
        pruned,
        cachedSegmentOnDay(3),
        cachedSegmentOnDay(2)
    );
    queue.start();

    Assertions.assertEquals(List.of(day(3), day(2)), handOutAll(queue));
    Assertions.assertEquals(0, pruned.countAcquires.get(), "a pruned segment must never be acquired");
    queue.close();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void testTimeOrderCachedSegmentJumpsAheadOnlyWithinItsInterval(final int loadahead)
  {
    // In time order: a loading segment, then a cached one with the same interval, then a cached one with an older
    // interval. The cached segment with the same interval is outside the loadahead window.
    final TestLoadableSegment loading = loadingSegmentOnDay(3, 0);
    final TestLoadableSegment cachedSameDay = cachedSegmentOnDay(3, 1);
    final TestLoadableSegment cachedOlderDay = cachedSegmentOnDay(2, 0);

    final ReadableInputQueue queue = makeQueue(
        loadahead,
        timeOrderedPruner(Order.DESCENDING, interval -> false),
        loading,
        cachedSameDay,
        cachedOlderDay
    );
    queue.start();
    Assertions.assertEquals(loadahead, loading.countAcquires.get(), "cached segments must not delay loadahead");

    // The cached segment level with the loading one goes first; the older one must wait for the loading one.
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> first = queue.nextInput();
    Assertions.assertTrue(first.isDone(), "a cached segment level with the head must be handed out first");
    Assertions.assertEquals(descriptorOnDay(3, 1), Futures.getUnchecked(first).input().getSegment().getDescriptor());
    Assertions.assertEquals(0, cachedOlderDay.countAcquires.get(), "an older interval must not be acquired early");
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> second = queue.nextInput();
    Assertions.assertFalse(second.isDone(), "the loading segment must go before a loaded segment from an older day");
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> third = queue.nextInput();
    Assertions.assertEquals(descriptorOnDay(2, 0), Futures.getUnchecked(third).input().getSegment().getDescriptor());

    closeHandedOut(first);
    closeHandedOut(third);
    queue.close();
  }

  @Test
  void testTimeOrderCachedSegmentsDoNotCountTowardLoadahead()
  {
    // Loadahead pulls day 3's completed load in behind day 4. Reaching day 3 acquires its two cached segments.
    final TestLoadableSegment completedLoad = new TestLoadableSegment(
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(new CountingSegment()))),
        Optional.empty(),
        false,
        descriptorOnDay(3, 0)
    );
    final TestLoadableSegment loading = loadingSegmentOnDay(3, 3);

    final ReadableInputQueue queue = makeQueue(
        1,
        timeOrderedPruner(Order.DESCENDING, interval -> false),
        cachedSegmentOnDay(4),
        completedLoad,
        cachedSegmentOnDay(3, 1),
        cachedSegmentOnDay(3, 2),
        loading
    );
    queue.start();

    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> day4Future = queue.nextInput();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> completedFuture = queue.nextInput();
    Assertions.assertEquals(
        descriptorOnDay(3, 0),
        Futures.getUnchecked(completedFuture).input().getSegment().getDescriptor()
    );
    Assertions.assertEquals(1, loading.countAcquires.get(), "handing out a load must start the next one");

    closeHandedOut(day4Future);
    closeHandedOut(completedFuture);
    queue.close();
  }

  @Test
  void testLoadaheadFailurePropagatesWhenHandedOut()
  {
    // A load-ahead acquisition that fails for a reason other than a full cache is not swallowed: it is handed out as a
    // failed future, so the stage fails promptly.
    final AcquireSegmentAction failed = new AcquireSegmentAction();
    failed.setException(new IllegalStateException("load went sideways"));
    final TestLoadableSegment loadable = new TestLoadableSegment(failed);

    final ReadableInputQueue queue = makeQueue(1, loadable);
    queue.start();
    Assertions.assertEquals(0, loadable.abandonCalls.get(), "non-capacity failures must not be deferred");
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertTrue(future.isDone());
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("load went sideways"));
    queue.close();
  }

  @Test
  void testLoadaheadSyncNonCapacityFailurePropagatesFromStart()
  {
    final TestLoadableSegment loadable = new TestLoadableSegment(new AcquireSegmentAction())
    {
      @Override
      public AcquireSegmentAction acquire(AcquireMode acquireMode)
      {
        throw new IllegalStateException("acquire went sideways");
      }
    };

    final ReadableInputQueue queue = makeQueue(1, loadable);
    final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, queue::start);
    Assertions.assertEquals("acquire went sideways", e.getMessage());
    queue.close();
  }

  @ParameterizedTest
  @EnumSource(NoRoomStyle.class)
  void testStartStopsLoadingAheadWhenCacheIsFull(final NoRoomStyle noRoomStyle) throws Exception
  {
    final TestSegmentCache cache = new TestSegmentCache(2);
    final List<CacheBackedLoadableSegment> segments = makeSegments(4, cache, noRoomStyle);
    final ReadableInputQueue queue = makeQueue(4, segments);

    // Cache holds 2 of the 4 requested load-aheads; the rest must be deferred rather than failing start(), and the
    // first deferral must stop load-ahead for now rather than trying (and failing) the remaining segments too.
    queue.start();
    Assertions.assertEquals(4, queue.remaining(), "deferral must not change the input count");
    Assertions.assertEquals(2, cache.getUsedSlots());
    Assertions.assertEquals(1, cache.getFailedReservations());

    // Every segment is still handed out exactly once.
    Assertions.assertEquals(descriptorsOf(segments), drainDescriptors(queue));
    Assertions.assertEquals(0, cache.getUsedSlots());
    queue.close();
  }

  @ParameterizedTest
  @EnumSource(NoRoomStyle.class)
  void testHandsOutEveryInputWhenCacheFitsOnlyOne(final NoRoomStyle noRoomStyle) throws Exception
  {
    // Room for a single segment at a time, with more load-ahead than that: every load-ahead beyond the one being
    // processed has to be deferred, but the queue must still work its way through all 8 segments.
    final TestSegmentCache cache = new TestSegmentCache(1);
    final List<CacheBackedLoadableSegment> segments = makeSegments(8, cache, noRoomStyle);
    final ReadableInputQueue queue = makeQueue(4, segments);

    queue.start();
    Assertions.assertEquals(descriptorsOf(segments), drainDescriptors(queue));
    Assertions.assertEquals(0, cache.getUsedSlots());
    Assertions.assertEquals(0, queue.remaining());
    queue.close();
  }

  @ParameterizedTest
  @EnumSource(NoRoomStyle.class)
  void testDeferredSegmentIsAcquiredAgainLater(final NoRoomStyle noRoomStyle) throws Exception
  {
    final TestSegmentCache cache = new TestSegmentCache(1);
    final List<CacheBackedLoadableSegment> segments = makeSegments(2, cache, noRoomStyle);
    final ReadableInputQueue queue = makeQueue(2, segments);

    queue.start();

    // First segment loaded ahead; second one had nowhere to go.
    Assertions.assertEquals(1, segments.get(0).acquireCount.get());
    Assertions.assertEquals(1, segments.get(1).acquireCount.get());
    Assertions.assertEquals(1, cache.getUsedSlots());
    // a failed action must be abandoned so the segment can be acquired again; a sync throw never acquired it
    Assertions.assertEquals(noRoomStyle == NoRoomStyle.FAIL_ACTION ? 1 : 0, segments.get(1).abandonCount.get());

    // Hand out and release the first, which frees room for the second to be acquired again.
    take(queue).close();
    Assertions.assertEquals(descriptorsOf(segments.subList(1, 2)), drainDescriptors(queue));
    Assertions.assertEquals(2, segments.get(1).acquireCount.get());
    queue.close();
  }

  /**
   * Load-ahead backs off exponentially while the cache stays full, so a query over many segments does not pay for a
   * doomed acquisition per input.
   */
  @ParameterizedTest
  @EnumSource(NoRoomStyle.class)
  void testLoadaheadBacksOffWhileCacheStaysFull(final NoRoomStyle noRoomStyle) throws Exception
  {
    // Room for two segments, one of which is reserved by something else for the whole test: only the input currently
    // being processed fits, so load-ahead can never succeed.
    final TestSegmentCache cache = new TestSegmentCache(2);
    final Closeable foreignReservation = cache.reserveSlot();
    final List<CacheBackedLoadableSegment> segments = makeSegments(40, cache, noRoomStyle);
    final ReadableInputQueue queue = makeQueue(4, segments);

    queue.start();
    Assertions.assertEquals(descriptorsOf(segments), drainDescriptors(queue));

    // Without backoff there would be a failed load-ahead for nearly every one of the 40 inputs. With it, the gaps
    // between attempts double: 1, 2, 4, 8, 16 inputs.
    Assertions.assertEquals(6, cache.getFailedReservations());

    queue.close();
    closeQuietly(foreignReservation);
    Assertions.assertEquals(0, cache.getUsedSlots());
  }

  @ParameterizedTest
  @EnumSource(NoRoomStyle.class)
  void testBackoffResetsAfterSuccessfulLoadahead(final NoRoomStyle noRoomStyle) throws Exception
  {
    final TestSegmentCache cache = new TestSegmentCache(2);
    final List<CacheBackedLoadableSegment> segments = makeSegments(8, cache, noRoomStyle);
    final ReadableInputQueue queue = makeQueue(1, segments);

    queue.start(); // segment 0 loaded ahead
    final SegmentReference ref0 = take(queue); // tops up: segment 1 loaded ahead
    final SegmentReference ref1 = take(queue); // tops up: segment 2 has no room -> cooldown 1
    Assertions.assertEquals(1, cache.getFailedReservations());
    ref0.close();
    ref1.close();

    final SegmentReference ref2 = take(queue); // on demand; burns the cooldown
    ref2.close();
    final SegmentReference ref3 = take(queue); // on demand; tops up: segment 4 loaded ahead -> backoff resets
    Assertions.assertEquals(1, segments.get(4).acquireCount.get());
    final SegmentReference ref4 = take(queue); // tops up: segment 5 has no room (3 and 4 held) -> cooldown 1 again
    Assertions.assertEquals(2, cache.getFailedReservations());
    ref3.close();
    ref4.close();

    final SegmentReference ref5 = take(queue); // on demand; burns the cooldown (would not, had backoff grown to 2)
    ref5.close();
    final SegmentReference ref6 = take(queue); // on demand; tops up: segment 7 loaded ahead
    Assertions.assertEquals(1, segments.get(7).acquireCount.get(), "backoff must reset after a successful load-ahead");
    ref6.close();

    Assertions.assertEquals(descriptorsOf(segments.subList(7, 8)), drainDescriptors(queue));
    queue.close();
  }

  @Test
  void testOnDemandSyncCapacityFailureFailsNextInput() throws Exception
  {
    // Load-ahead is disabled, so each input is acquired on demand. Acquiring on demand means the stage has nothing
    // else to work on, so there is nothing to relax and running out of room does fail the query.
    final TestSegmentCache cache = new TestSegmentCache(1);
    final List<CacheBackedLoadableSegment> segments = makeSegments(2, cache, NoRoomStyle.THROW);
    final ReadableInputQueue queue = makeQueue(0, segments);

    queue.start();
    final SegmentReference ref0 = take(queue); // holds the only slot
    final DruidException e = Assertions.assertThrows(DruidException.class, queue::nextInput);
    Assertions.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    ref0.close();
    queue.close();
  }

  @Test
  void testOnDemandAsyncCapacityFailureFailsFuture() throws Exception
  {
    final TestSegmentCache cache = new TestSegmentCache(1);
    final List<CacheBackedLoadableSegment> segments = makeSegments(2, cache, NoRoomStyle.FAIL_ACTION);
    final ReadableInputQueue queue = makeQueue(0, segments);

    queue.start();
    final SegmentReference ref0 = take(queue); // holds the only slot
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertEquals(
        DruidException.Category.CAPACITY_EXCEEDED,
        ((DruidException) e.getCause()).getCategory()
    );
    Assertions.assertEquals(0, segments.get(1).abandonCount.get(), "on-demand failures are not deferred");
    Assertions.assertEquals(0, queue.remaining(), "on-demand failures do not put the segment back");
    ref0.close();
    queue.close();
  }

  @Test
  void testClaimedLoadaheadThatFailsForCapacityFailsTheFuture() throws Exception
  {
    // The load-ahead acquisition was handed out before it completed, so a processor is waiting on it: it is needed to
    // make progress and may not be deferred.
    final TestSegmentCache cache = new TestSegmentCache(1);
    final Closeable foreignReservation = cache.reserveSlot();
    final CacheBackedLoadableSegment segment = new CacheBackedLoadableSegment(0, cache, NoRoomStyle.FAIL_ACTION, true);
    final ReadableInputQueue queue = makeQueue(1, List.of(segment));

    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertFalse(future.isDone());

    segment.completeLoad(); // no room
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertEquals(
        DruidException.Category.CAPACITY_EXCEEDED,
        ((DruidException) e.getCause()).getCategory()
    );
    Assertions.assertEquals(0, segment.abandonCount.get());
    Assertions.assertEquals(0, queue.remaining());
    closeQuietly(foreignReservation);
    queue.close();
  }

  @Test
  void testPendingLoadaheadThatFailsForCapacityIsDeferred() throws Exception
  {
    // Nobody is waiting on the load-ahead acquisition when it fails for lack of room, so it is unwound and the segment
    // goes back to be tried again.
    final TestSegmentCache cache = new TestSegmentCache(1);
    final Closeable foreignReservation = cache.reserveSlot();
    final CacheBackedLoadableSegment segment = new CacheBackedLoadableSegment(0, cache, NoRoomStyle.FAIL_ACTION, true);
    final ReadableInputQueue queue = makeQueue(1, List.of(segment));

    queue.start();
    Assertions.assertEquals(1, segment.acquireCount.get());
    segment.completeLoad(); // no room
    Assertions.assertEquals(1, segment.abandonCount.get());
    Assertions.assertEquals(1, queue.remaining(), "deferred segment must still be counted");

    // Room frees up; the next request acquires the segment on demand and gets it.
    closeQuietly(foreignReservation);
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertEquals(2, segment.acquireCount.get());
    Assertions.assertFalse(future.isDone());
    segment.completeLoad();
    final SegmentReference ref = future.get(10, TimeUnit.SECONDS).input().getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    ref.close();
    Assertions.assertNull(queue.nextInput());
    queue.close();
  }

  @Test
  void testLoadaheadFailingAfterCloseIsNotDeferred()
  {
    // close() claims the in-flight load-ahead; a capacity failure arriving afterwards must not put anything back.
    final TestSegmentCache cache = new TestSegmentCache(1);
    final Closeable foreignReservation = cache.reserveSlot();
    final CacheBackedLoadableSegment segment = new CacheBackedLoadableSegment(0, cache, NoRoomStyle.FAIL_ACTION, true);
    final ReadableInputQueue queue = makeQueue(1, List.of(segment));

    queue.start();
    queue.close();
    segment.completeLoad(); // no room, but the action was closed already
    Assertions.assertEquals(0, segment.abandonCount.get());
    Assertions.assertEquals(0, queue.remaining());
    Assertions.assertNull(queue.nextInput());
    Assertions.assertEquals(1, segment.acquireCount.get());
    closeQuietly(foreignReservation);
    Assertions.assertEquals(0, cache.getUsedSlots());
  }

  @ParameterizedTest
  @EnumSource(NoRoomStyle.class)
  void testCloseReleasesEverythingNotHandedOut(final NoRoomStyle noRoomStyle)
  {
    final TestSegmentCache cache = new TestSegmentCache(2);
    final List<CacheBackedLoadableSegment> segments = makeSegments(4, cache, noRoomStyle);
    final ReadableInputQueue queue = makeQueue(4, segments);

    queue.start();
    Assertions.assertEquals(2, cache.getUsedSlots());

    queue.close();
    Assertions.assertEquals(0, cache.getUsedSlots());
    Assertions.assertEquals(0, queue.remaining());
    Assertions.assertNull(queue.nextInput());
    for (final CacheBackedLoadableSegment segment : segments) {
      Assertions.assertTrue(segment.acquireCount.get() <= 1, "deferred segments must not be retried after close");
    }
  }

  @Test
  void testCachedSegmentNeedingRoomIsAcquiredLater() throws Exception
  {
    // A locally-cached segment can still need room in the cache (e.g. to re-reserve evicted parts). start() must not
    // fail on it; it is treated as not cached and acquired later like any other segment.
    final TestSegmentCache cache = new TestSegmentCache(1);
    final CacheBackedLoadableSegment segment = new CacheBackedLoadableSegment(0, cache, NoRoomStyle.THROW, false)
    {
      @Override
      public Optional<Segment> acquireIfCached(AcquireMode acquireMode)
      {
        throw noRoom(descriptor());
      }
    };
    final ReadableInputQueue queue = makeQueue(1, List.of(segment));

    queue.start();
    Assertions.assertEquals(1, queue.remaining());
    Assertions.assertEquals(1, segment.acquireCount.get(), "load-ahead must acquire the not-really-cached segment");
    Assertions.assertEquals(descriptorsOf(List.of(segment)), drainDescriptors(queue));
    queue.close();
  }

  @ParameterizedTest
  @EnumSource(NoRoomStyle.class)
  void testTimeOrderSurvivesDeferredLoadahead(final NoRoomStyle noRoomStyle) throws Exception
  {
    // Deferred segments go back to their place in the queue, so segments are still handed out in time order while the
    // cache is too small for load-ahead to keep up.
    final TestSegmentCache cache = new TestSegmentCache(2);
    final List<CacheBackedLoadableSegment> segments = new ArrayList<>();
    for (final int day : new int[]{3, 6, 1, 5, 2, 4}) {
      segments.add(new CacheBackedLoadableSegment(descriptorOnDay(day, 0), cache, noRoomStyle, false));
    }
    final ReadableInputQueue queue = makeQueue(
        3,
        timeOrderedPruner(Order.DESCENDING, interval -> false),
        segments.toArray(new LoadableSegment[0])
    );

    queue.start();
    Assertions.assertEquals(1, cache.getFailedReservations(), "start() must stop at the first deferral");
    final List<SegmentDescriptor> expected = new ArrayList<>();
    for (int day = 6; day >= 1; day--) {
      expected.add(descriptorOnDay(day, 0));
    }
    Assertions.assertEquals(expected, drainInOrder(queue));
    Assertions.assertTrue(cache.getFailedReservations() > 1, "load-ahead must have kept hitting the full cache");
    Assertions.assertEquals(0, cache.getUsedSlots());
    queue.close();
  }

  /**
   * Several load-aheads are in flight when they find the cache full, and their failures arrive one at a time, in the
   * order the acquisitions started. Each deferred segment must go back to its own place in hand-out order (pushing each
   * onto the front would reverse them), and the round of failures must back load-ahead off once, not once per failure.
   */
  @ParameterizedTest
  @EnumSource(Order.class)
  void testAsyncDeferralsKeepHandOutOrderAndBackOffOnce(final Order order) throws Exception
  {
    final TestSegmentCache cache = new TestSegmentCache(4);
    final List<Closeable> foreignReservations = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      foreignReservations.add(cache.reserveSlot());
    }
    final int[] inputDays = {3, 1, 4, 2};
    final List<CacheBackedLoadableSegment> segments = new ArrayList<>();
    for (final int day : inputDays) {
      segments.add(new CacheBackedLoadableSegment(descriptorOnDay(day, 0), cache, NoRoomStyle.FAIL_ACTION, true));
    }
    final ReadableInputQueue queue = makeQueue(
        3,
        timeOrderedPruner(order, interval -> false),
        segments.toArray(new LoadableSegment[0])
    );

    final int[] expectedDays = switch (order) {
      case ASCENDING -> new int[]{1, 2, 3, 4};
      case DESCENDING -> new int[]{4, 3, 2, 1};
      case NONE -> inputDays;
    };
    final List<SegmentDescriptor> expected = new ArrayList<>();
    final List<CacheBackedLoadableSegment> inHandOutOrder = new ArrayList<>();
    for (final int day : expectedDays) {
      expected.add(descriptorOnDay(day, 0));
      for (final CacheBackedLoadableSegment segment : segments) {
        if (segment.descriptor().equals(descriptorOnDay(day, 0))) {
          inHandOutOrder.add(segment);
        }
      }
    }

    // Load-ahead starts the first three, and their loads all find the cache full, in the order they started.
    queue.start();
    Assertions.assertEquals(List.of(1, 1, 1, 0), acquireCounts(inHandOutOrder));
    completePendingLoads(inHandOutOrder);
    Assertions.assertEquals(3, cache.getFailedReservations());
    Assertions.assertEquals(List.of(1, 1, 1, 0), abandonCounts(inHandOutOrder));
    Assertions.assertEquals(4, queue.remaining(), "deferral must not change the input count");

    foreignReservations.forEach(ReadableInputQueueTest::closeQuietly);
    final List<SegmentDescriptor> handedOut = new ArrayList<>();

    // The first input is acquired on demand, which burns the cooldown. One round of failures backs off for one input.
    handedOut.add(takeCompletingLoads(queue, segments));
    Assertions.assertEquals(List.of(2, 1, 1, 0), acquireCounts(inHandOutOrder));

    // The second input is acquired on demand too, after which load-ahead resumes and picks up the other two. Had each
    // failure doubled the backoff, the cooldown would have been 4 inputs long, and load-ahead would still be idle.
    handedOut.add(takeCompletingLoads(queue, segments));
    Assertions.assertEquals(List.of(2, 2, 2, 1), acquireCounts(inHandOutOrder));

    handedOut.add(takeCompletingLoads(queue, segments));
    handedOut.add(takeCompletingLoads(queue, segments));
    Assertions.assertNull(queue.nextInput());
    Assertions.assertEquals(expected, handedOut);
    Assertions.assertEquals(3, cache.getFailedReservations());
    Assertions.assertEquals(0, cache.getUsedSlots());
    queue.close();
  }

  @Test
  void testLoadaheadFailureWhileHandingOutDoesNotCountTheInput()
  {
    // Handing out the first segment tops up load-ahead, which fails for a reason other than a full cache. nextInput()
    // throws, so the first segment is never handed out and must not be counted as read; close() releases it instead.
    final CountingSegment firstSegment = new CountingSegment();
    final TestLoadableSegment first = new TestLoadableSegment(
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(firstSegment)))
    );
    final TestLoadableSegment second = new TestLoadableSegment(new AcquireSegmentAction())
    {
      @Override
      public AcquireSegmentAction acquire(AcquireMode acquireMode)
      {
        throw new IllegalStateException("acquire went sideways");
      }
    };
    final ReadableInputQueue queue = makeQueue(1, first, second);

    queue.start();
    final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, queue::nextInput);
    Assertions.assertEquals("acquire went sideways", e.getMessage());
    Assertions.assertEquals(0, first.countHandedOutCalls.get(), "an input that was not handed out must not be counted");

    queue.close();
    Assertions.assertEquals(1, firstSegment.closes.get());
  }

  @Test
  void testCloseBeforeOnDemandLoadIsAwaitedFailsItsFuture()
  {
    // close() claims and closes the load between acquire() and the queue registering to await it: registering then
    // fails, which must not fail nextInput(), and the future handed out fails like any load in flight at close.
    final AtomicReference<ReadableInputQueue> queueRef = new AtomicReference<>();
    final AtomicInteger cancels = new AtomicInteger();
    final TestLoadableSegment loadable = new TestLoadableSegment(
        new HookedAcquireSegmentAction(cancels::incrementAndGet, () -> queueRef.get().close())
    );
    final ReadableInputQueue queue = makeQueue(0, loadable);
    queueRef.set(queue);

    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("Input queue closed"), e.getCause().getMessage());
    Assertions.assertEquals(1, cancels.get(), "canceler must run exactly once");
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get());
    Assertions.assertEquals(0, loadable.countHandedOutCalls.get());
    Assertions.assertEquals(0, queue.remaining());
  }

  @Test
  void testCloseBeforeLoadaheadIsAwaitedDoesNotFailStart()
  {
    final AtomicReference<ReadableInputQueue> queueRef = new AtomicReference<>();
    final AtomicInteger cancels = new AtomicInteger();
    final TestLoadableSegment loadable = new TestLoadableSegment(
        new HookedAcquireSegmentAction(cancels::incrementAndGet, () -> queueRef.get().close())
    );
    final ReadableInputQueue queue = makeQueue(1, loadable);
    queueRef.set(queue);

    queue.start();
    Assertions.assertEquals(1, cancels.get(), "canceler must run exactly once");
    Assertions.assertEquals(0, queue.remaining());
    Assertions.assertNull(queue.nextInput());
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get());
  }

  @Test
  void testSkipBeforeLoadaheadIsAwaitedCancelsIt()
  {
    // Every input becomes prunable between acquire() and the queue registering to await the load-ahead, and a
    // nextInput() call in that window releases the pending load-ahead. start() must not fail on it.
    final AtomicReference<ReadableInputQueue> queueRef = new AtomicReference<>();
    final AtomicBoolean skipAll = new AtomicBoolean(false);
    final List<ListenableFuture<ReadableInputQueue.NumberedReadableInput>> nestedInputs = new ArrayList<>();
    final AtomicInteger cancels = new AtomicInteger();
    final TestLoadableSegment loadable = new TestLoadableSegment(
        new HookedAcquireSegmentAction(
            cancels::incrementAndGet,
            () -> {
              skipAll.set(true);
              nestedInputs.add(queueRef.get().nextInput());
            }
        )
    );
    final ReadableInputQueue queue = makeQueue(1, interval -> skipAll.get(), loadable);
    queueRef.set(queue);

    queue.start();
    Assertions.assertEquals(1, nestedInputs.size());
    Assertions.assertNull(nestedInputs.get(0), "nothing may be handed out once every input can be skipped");
    Assertions.assertEquals(1, cancels.get(), "skipped load must be canceled exactly once");
    Assertions.assertEquals(0, queue.remaining());
    Assertions.assertNull(queue.nextInput());

    queue.close();
    Assertions.assertEquals(1, cancels.get(), "skipped load must not be canceled again on close");
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get());
  }

  /**
   * A load-ahead that finds no room, but cannot be abandoned, cannot be deferred either: its capacity failure stands,
   * with the abandon failure attached as suppressed, and it is handed out in turn so the query fails cleanly. Neither
   * start() nor the producer completing the load sees the abandon failure, and the future is not left hanging.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLoadaheadThatCannotBeAbandonedFailsWithItsCapacityFailure(final boolean manual) throws Exception
  {
    final TestSegmentCache cache = new TestSegmentCache(1);
    final Closeable foreignReservation = cache.reserveSlot();
    final CacheBackedLoadableSegment segment =
        new CacheBackedLoadableSegment(0, cache, NoRoomStyle.FAIL_ACTION, manual)
        {
          @Override
          public synchronized void abandonFailedAcquisition()
          {
            throw new IllegalStateException("abandon went sideways");
          }
        };
    final ReadableInputQueue queue = makeQueue(1, List.of(segment));

    // Without manual mode, the load fails right away, so the ready callback runs inline in start().
    queue.start();
    if (manual) {
      segment.completeLoad(); // no room
    }
    Assertions.assertEquals(1, queue.remaining(), "the failed load-ahead must still be pending");

    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertTrue(future.isDone(), "the failed load-ahead must not be left hanging");
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    final DruidException failure = Assertions.assertInstanceOf(DruidException.class, e.getCause());
    Assertions.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, failure.getCategory());
    Assertions.assertEquals(1, failure.getSuppressed().length);
    Assertions.assertEquals("abandon went sideways", failure.getSuppressed()[0].getMessage());
    Assertions.assertEquals(1, segment.acquireCount.get(), "a load-ahead that was not deferred must not be retried");
    Assertions.assertEquals(0, queue.remaining());

    queue.close();
    closeQuietly(foreignReservation);
    Assertions.assertEquals(0, cache.getUsedSlots());
  }

  @Test
  void testInlineDeliveryFailureSurfacesFromStart() throws Exception
  {
    // Delivering a load that is ready already runs inline while the queue registers to await it. If delivery itself
    // throws (here, logging the failure to close the failed action), that must surface rather than be mistaken for a
    // load that was closed elsewhere, which would swallow it and leave the load-ahead's future to hang until close().
    final AcquireSegmentAction failed = new AcquireSegmentAction()
    {
      @Override
      public void close()
      {
        super.close();
        throw new IllegalStateException("close went sideways");
      }
    };
    failed.setException(new IllegalStateException("load went sideways"));
    final AtomicBoolean describedOnce = new AtomicBoolean(false);
    final TestLoadableSegment loadable = new TestLoadableSegment(failed)
    {
      @Override
      public String description()
      {
        // Throws only the first time, which is while delivering: anything logged afterwards, such as a swallowed
        // failure, must not throw too, or it would mask the swallowing.
        if (describedOnce.compareAndSet(false, true)) {
          throw new IllegalStateException("description went sideways");
        }
        return super.description();
      }
    };
    final ReadableInputQueue queue = makeQueue(1, loadable);

    final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, queue::start);
    Assertions.assertEquals("description went sideways", e.getMessage());

    // The delivery stopped partway, so the pending future never completes by itself; close() still fails it.
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertFalse(future.isDone());
    queue.close();
    final ExecutionException closeFailure = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(
        closeFailure.getCause().getMessage().contains("Input queue closed"),
        closeFailure.getCause().getMessage()
    );
  }

  @Test
  void testTimeOrderDeferredSegmentPrunedLaterIsNotAcquiredAgain() throws Exception
  {
    // A deferred segment is back in the queue rather than pending, so if it becomes prunable it is simply dropped when
    // reached: it is neither released as a skipped input nor acquired again.
    final TestSegmentCache cache = new TestSegmentCache(1);
    final Closeable foreignReservation = cache.reserveSlot();
    final CacheBackedLoadableSegment day3 =
        new CacheBackedLoadableSegment(descriptorOnDay(3, 0), cache, NoRoomStyle.FAIL_ACTION, true);
    final CacheBackedLoadableSegment day2 =
        new CacheBackedLoadableSegment(descriptorOnDay(2, 0), cache, NoRoomStyle.FAIL_ACTION, true);
    final AtomicBoolean skipDay3 = new AtomicBoolean(false);
    final ReadableInputQueue queue = makeQueue(
        1,
        timeOrderedPruner(Order.DESCENDING, interval -> skipDay3.get() && interval.equals(day(3))),
        day2,
        day3
    );

    queue.start();
    Assertions.assertEquals(1, day3.acquireCount.get());
    day3.completeLoad(); // no room
    Assertions.assertEquals(1, day3.abandonCount.get());
    Assertions.assertEquals(2, queue.remaining());

    skipDay3.set(true);
    closeQuietly(foreignReservation);
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    day2.completeLoad();
    final SegmentReference ref = future.get(10, TimeUnit.SECONDS).input().getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    Assertions.assertEquals(descriptorOnDay(2, 0), ref.getSegmentDescriptor());
    ref.close();

    Assertions.assertNull(queue.nextInput());
    Assertions.assertEquals(1, day3.acquireCount.get(), "a pruned deferred segment must not be acquired again");
    queue.close();
    Assertions.assertEquals(0, cache.getUsedSlots());
  }

  @Test
  void testTimeOrderCachedSegmentNeedingRoomIsLoadedInItsGroup() throws Exception
  {
    // Probing day 3 for cached segments hits one whose cached acquisition needs room the cache does not have. It is
    // treated as not cached and loaded, still ahead of day 2.
    final TestSegmentCache cache = new TestSegmentCache(1);
    final CacheBackedLoadableSegment needsRoom =
        new CacheBackedLoadableSegment(descriptorOnDay(3, 1), cache, NoRoomStyle.THROW, false)
        {
          @Override
          public Optional<Segment> acquireIfCached(AcquireMode acquireMode)
          {
            throw noRoom(descriptor());
          }
        };
    final ReadableInputQueue queue = makeQueue(
        1,
        timeOrderedPruner(Order.DESCENDING, interval -> false),
        cachedSegmentOnDay(2, 0),
        cachedSegmentOnDay(3, 0),
        needsRoom,
        cachedSegmentOnDay(3, 2)
    );

    queue.start();
    Assertions.assertEquals(1, needsRoom.acquireCount.get());
    Assertions.assertEquals(
        List.of(descriptorOnDay(3, 0), descriptorOnDay(3, 2), descriptorOnDay(3, 1), descriptorOnDay(2, 0)),
        drainInOrder(queue)
    );
    queue.close();
  }

  @Test
  void testCachedGroupProbeFailureLosesNoSegments()
  {
    // A non-capacity failure while probing a group for cached segments fails start(), but every segment the probe
    // polled is still accounted for, and close() releases the one it acquired.
    final CountingSegment acquiredFirst = new CountingSegment();
    final TestLoadableSegment first =
        new TestLoadableSegment(new AcquireSegmentAction(), Optional.of(acquiredFirst), false, descriptorOnDay(1, 0));
    final TestLoadableSegment broken =
        new TestLoadableSegment(new AcquireSegmentAction(), Optional.empty(), false, descriptorOnDay(1, 1))
        {
          @Override
          public Optional<Segment> acquireIfCached(AcquireMode acquireMode)
          {
            throw new IllegalStateException("probe went sideways");
          }
        };
    final TestLoadableSegment last = cachedSegmentOnDay(1, 2);

    final ReadableInputQueue queue = makeQueue(0, first, broken, last);
    final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, queue::start);
    Assertions.assertEquals("probe went sideways", e.getMessage());
    Assertions.assertEquals(3, queue.remaining(), "segments polled by the probe must not be lost");
    Assertions.assertEquals(0, last.countAcquires.get());

    queue.close();
    Assertions.assertEquals(1, acquiredFirst.closes.get());
  }

  private static Interval day(final int day)
  {
    return Intervals.of("2026-01-%02d/2026-01-%02d", day, day + 1);
  }

  private static SegmentDescriptor descriptorOnDay(final int day, final int partition)
  {
    return new SegmentDescriptor(day(day), "v1", partition);
  }

  private static TestLoadableSegment cachedSegmentOnDay(final int day)
  {
    return cachedSegmentOnDay(day, 0);
  }

  private static TestLoadableSegment cachedSegmentOnDay(final int day, final int partition)
  {
    return new TestLoadableSegment(
        new AcquireSegmentAction(),
        Optional.of(new CountingSegment()),
        false,
        descriptorOnDay(day, partition)
    );
  }

  /**
   * Returns an uncached segment whose load never completes.
   */
  private static TestLoadableSegment loadingSegmentOnDay(final int day, final int partition)
  {
    return new TestLoadableSegment(
        new AcquireSegmentAction(() -> {}),
        Optional.empty(),
        false,
        descriptorOnDay(day, partition)
    );
  }

  private static InputPruner timeOrderedPruner(final Order order, final Predicate<Interval> canSkip)
  {
    return new InputPruner()
    {
      @Override
      public boolean canSkip(final Interval interval)
      {
        return canSkip.test(interval);
      }

      @Override
      public Order getTimeOrder()
      {
        return order;
      }
    };
  }

  /**
   * Hands out every input, all of which must be ready, and returns their intervals in order. Closes each one.
   */
  private static List<Interval> handOutAll(final ReadableInputQueue queue)
  {
    final List<Interval> intervals = new ArrayList<>();
    ListenableFuture<ReadableInputQueue.NumberedReadableInput> future;
    while ((future = queue.nextInput()) != null) {
      Assertions.assertTrue(future.isDone(), "cached segments must be ready");
      intervals.add(Futures.getUnchecked(future).input().getSegment().getDescriptor().getInterval());
      closeHandedOut(future);
    }
    return intervals;
  }

  private static void closeHandedOut(final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future)
  {
    CloseableUtils.closeAndWrapExceptions(
        Futures.getUnchecked(future).input().getSegment().getSegmentReferenceOnce()
    );
  }

  /**
   * How a {@link CacheBackedLoadableSegment} reports that the segment cache has no room for it. Real
   * {@link LoadableSegment} implementations do it both ways: {@link org.apache.druid.msq.input.RegularLoadableSegment}
   * reserves space inline when it has the {@link DataSegment} already (throwing on failure), and asynchronously when it
   * must fetch it from the Coordinator first, or when reserving the bundles of a partial-download segment (failing the
   * action).
   */
  private enum NoRoomStyle
  {
    THROW,
    FAIL_ACTION
  }

  /**
   * Fake segment cache with a fixed number of slots. Each acquired segment occupies one slot until it is closed.
   */
  private static class TestSegmentCache
  {
    private final int slots;
    private int usedSlots;
    private int failedReservations;

    TestSegmentCache(int slots)
    {
      this.slots = slots;
    }

    @Nullable
    synchronized Closeable reserveSlot()
    {
      if (usedSlots >= slots) {
        failedReservations++;
        return null;
      }
      usedSlots++;
      final AtomicBoolean released = new AtomicBoolean(false);
      return () -> {
        if (released.compareAndSet(false, true)) {
          synchronized (this) {
            usedSlots--;
          }
        }
      };
    }

    synchronized int getUsedSlots()
    {
      return usedSlots;
    }

    synchronized int getFailedReservations()
    {
      return failedReservations;
    }
  }

  private static DruidException noRoom(final SegmentDescriptor descriptor)
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                         .build("No room in the segment cache for segment[%s]", descriptor);
  }

  /**
   * {@link LoadableSegment} whose acquisition needs a slot in a {@link TestSegmentCache}, reporting a full cache in the
   * given {@link NoRoomStyle}. In manual mode, an acquisition stays pending until {@link #completeLoad()} is called.
   */
  private static class CacheBackedLoadableSegment implements LoadableSegment
  {
    private final SegmentDescriptor descriptor;
    private final TestSegmentCache cache;
    private final NoRoomStyle noRoomStyle;
    private final boolean manual;
    final AtomicInteger acquireCount = new AtomicInteger();
    final AtomicInteger abandonCount = new AtomicInteger();

    private boolean acquired;
    @Nullable
    private AcquireSegmentAction pendingAction;
    @Nullable
    private Closeable pendingSlot;

    CacheBackedLoadableSegment(int segmentNumber, TestSegmentCache cache, NoRoomStyle noRoomStyle, boolean manual)
    {
      this(
          SegmentId.of("foo", Intervals.of("2000/2001"), "v", segmentNumber).toDescriptor(),
          cache,
          noRoomStyle,
          manual
      );
    }

    CacheBackedLoadableSegment(
        SegmentDescriptor descriptor,
        TestSegmentCache cache,
        NoRoomStyle noRoomStyle,
        boolean manual
    )
    {
      this.descriptor = descriptor;
      this.cache = cache;
      this.noRoomStyle = noRoomStyle;
      this.manual = manual;
    }

    @Override
    public SegmentDescriptor descriptor()
    {
      return descriptor;
    }

    @Nullable
    @Override
    public String description()
    {
      return descriptor.toString();
    }

    @Override
    public Optional<Segment> acquireIfCached(AcquireMode acquireMode)
    {
      return Optional.empty();
    }

    @Override
    public synchronized AcquireSegmentAction acquire(AcquireMode acquireMode)
    {
      if (acquired) {
        throw DruidException.defensive("Segment[%s] is already acquired", descriptor);
      }
      acquireCount.incrementAndGet();

      final Closeable slot;
      if (noRoomStyle == NoRoomStyle.THROW) {
        // reserve inline, before the segment counts as acquired
        slot = cache.reserveSlot();
        if (slot == null) {
          throw noRoom(descriptor);
        }
      } else {
        slot = null;
      }

      acquired = true;
      final AcquireSegmentAction action = new AcquireSegmentAction(() -> closeQuietly(slot));
      if (manual) {
        pendingAction = action;
        pendingSlot = slot;
      } else {
        complete(action, slot);
      }
      return action;
    }

    /**
     * Completes a manual-mode acquisition: delivers the segment if the cache has room (or the slot was reserved up
     * front), else fails the action.
     */
    synchronized void completeLoad()
    {
      final AcquireSegmentAction action = Objects.requireNonNull(pendingAction, "no pending acquisition");
      final Closeable slot = pendingSlot;
      pendingAction = null;
      pendingSlot = null;
      complete(action, slot);
    }

    /**
     * Whether a manual-mode acquisition is waiting for {@link #completeLoad()}.
     */
    synchronized boolean hasPendingLoad()
    {
      return pendingAction != null;
    }

    private void complete(final AcquireSegmentAction action, @Nullable final Closeable reservedSlot)
    {
      final Closeable slot = reservedSlot != null ? reservedSlot : cache.reserveSlot();
      if (slot == null) {
        action.setException(noRoom(descriptor));
        return;
      }
      final AcquireSegmentResult result =
          AcquireSegmentResult.of(Optional.of(new CountingSegment(() -> closeQuietly(slot))));
      if (!action.set(result)) {
        // the action was closed before delivery, so the result is ours to close
        result.close();
      }
    }

    @Override
    public synchronized void abandonFailedAcquisition()
    {
      if (!acquired) {
        throw DruidException.defensive("Segment[%s] is not acquired", descriptor);
      }
      acquired = false;
      pendingAction = null;
      pendingSlot = null;
      abandonCount.incrementAndGet();
    }

    @Override
    public void countHandedOut()
    {
      // not counted
    }

    @Override
    public ListenableFuture<DataSegment> dataSegmentFuture()
    {
      return Futures.immediateFailedFuture(DruidException.defensive("not available"));
    }
  }

  /**
   * {@link AcquireSegmentAction} that runs a hook just before its ready callback is registered: after the queue has
   * acquired the segment, but before it starts awaiting the acquisition.
   */
  private static class HookedAcquireSegmentAction extends AcquireSegmentAction
  {
    private final Runnable beforeAddReadyCallback;

    HookedAcquireSegmentAction(final Runnable canceler, final Runnable beforeAddReadyCallback)
    {
      super(canceler);
      this.beforeAddReadyCallback = beforeAddReadyCallback;
    }

    @Override
    public void addReadyCallback(final Runnable callback)
    {
      beforeAddReadyCallback.run();
      super.addReadyCallback(callback);
    }
  }
}
