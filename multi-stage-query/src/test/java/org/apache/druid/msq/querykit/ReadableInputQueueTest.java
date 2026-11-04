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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
      closes.incrementAndGet();
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
    public ListenableFuture<DataSegment> dataSegmentFuture()
    {
      return Futures.immediateFailedFuture(DruidException.defensive("not available"));
    }
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
    // release succeeded, so the delivery was counted even though the downstream handoff then failed
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get());

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
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "acquireIfCached counts inline, not via the hook");
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
}
