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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link ReadableInputQueue}'s segment-load lifecycle: close-while-loading must promptly fail the futures
 * handed to frame processors (ready callbacks are dropped on close-before-ready, so this is load-bearing), delivered
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
    private final boolean throwOnCountDelivered;
    final AtomicInteger countDeliveredCalls = new AtomicInteger();

    TestLoadableSegment(AcquireSegmentAction action)
    {
      this(action, Optional.empty(), false);
    }

    TestLoadableSegment(AcquireSegmentAction action, Optional<Segment> cached)
    {
      this(action, cached, false);
    }

    TestLoadableSegment(AcquireSegmentAction action, Optional<Segment> cached, boolean throwOnCountDelivered)
    {
      this.action = action;
      this.cached = cached;
      this.throwOnCountDelivered = throwOnCountDelivered;
    }

    @Override
    public SegmentDescriptor descriptor()
    {
      return DESCRIPTOR;
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
      return cached;
    }

    @Override
    public AcquireSegmentAction acquire(AcquireMode acquireMode)
    {
      return action;
    }

    @Override
    public void countDelivered(AcquireSegmentResult result)
    {
      countDeliveredCalls.incrementAndGet();
      if (throwOnCountDelivered) {
        throw new IllegalStateException("countDelivered blew up");
      }
    }

    @Override
    public ListenableFuture<DataSegment> dataSegmentFuture()
    {
      return Futures.immediateFailedFuture(DruidException.defensive("not available"));
    }
  }

  private static ReadableInputQueue makeQueue(int loadahead, LoadableSegment... segments)
  {
    return new ReadableInputQueue(
        null,
        List.of(new PhysicalInputSlice(ReadablePartitions.empty(), List.of(segments), List.of())),
        loadahead,
        AcquireMode.FULL
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
    final ListenableFuture<ReadableInput> future = queue.nextInput();
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
    final List<ListenableFuture<ReadableInput>> futures = new ArrayList<>();
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
      final ListenableFuture<ReadableInput> future = queue.nextInput();
      Assertions.assertNotNull(future);
      Assertions.assertFalse(future.isDone());
      futures.add(future);
    }

    // Must not throw ConcurrentModificationException.
    queue.close();

    for (final ListenableFuture<ReadableInput> future : futures) {
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
    // release() succeeds, then a later step (countDelivered) throws: the released result must be closed, not leaked
    // (closing the RELEASED action is a no-op, so the segment + its folded holds would otherwise leak forever).
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed, Optional.empty(), true);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);

    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("countDelivered blew up"), e.getCause().getMessage());
    Assertions.assertEquals(1, segment.closes.get(), "post-release failure must close the delivered segment");

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
    final ListenableFuture<ReadableInput> future = queue.nextInput();
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
    final ListenableFuture<ReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final ReadableInput input = future.get(10, TimeUnit.SECONDS);
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
    final ListenableFuture<ReadableInput> future = queue.nextInput();
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
    final ListenableFuture<ReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final ReadableInput input = future.get(10, TimeUnit.SECONDS);
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
    final ListenableFuture<ReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertTrue(future.isDone());
    final ReadableInput input = future.get(10, TimeUnit.SECONDS);
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
}
