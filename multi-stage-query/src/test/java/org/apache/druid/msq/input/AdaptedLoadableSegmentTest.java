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

package org.apache.druid.msq.input;

import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.common.asyncresource.SettableAsyncResource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link AdaptedLoadableSegment}'s forward bridge: a combinator-produced (non-releasable)
 * {@code AsyncResource<AcquireSegmentResult>} folded into the releasable {@link AcquireSegmentAction} handle, with
 * the resource chain's close riding the delivered segment's close.
 */
class AdaptedLoadableSegmentTest
{
  private static final SegmentDescriptor DESCRIPTOR = SegmentId.dummy("adapted").toDescriptor();

  private static class TestSegment implements Segment
  {
    final AtomicInteger closes = new AtomicInteger();

    @Override
    public SegmentId getId()
    {
      return SegmentId.dummy("adapted");
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

  /**
   * Mirrors the {@code ExternalInputSliceReader} chain shape: collect over source resources, transform to a result
   * whose segment is delivered unmanaged (the chain owns the sources).
   */
  private static AsyncResource<AcquireSegmentResult> makeChain(
      final TestSegment segment,
      final List<SettableAsyncResource<String>> sources
  )
  {
    return AsyncResources.transform(
        AsyncResources.collect(List.copyOf(sources)),
        files -> new AcquireSegmentResult(ReferenceCountedSegmentProvider.unmanaged(segment), 0L, 0L, 0L)
    );
  }

  @Test
  void testSegmentCloseClosesChainSourcesExactlyOnce() throws Exception
  {
    final TestSegment segment = new TestSegment();
    final AtomicInteger sourceCloses = new AtomicInteger();
    final SettableAsyncResource<String> source = new SettableAsyncResource<>();
    source.set("file", sourceCloses::incrementAndGet);

    final AdaptedLoadableSegment adapted =
        new AdaptedLoadableSegment(() -> makeChain(segment, List.of(source)), DESCRIPTOR, "test", null);
    final AcquireSegmentAction action = adapted.acquire(AcquireMode.FULL);
    action.await();
    final AcquireSegmentResult result = action.release();
    action.close();

    final Segment delivered = result.getSegment().orElseThrow();
    Assertions.assertEquals(0, sourceCloses.get(), "sources must outlive the delivered segment");

    delivered.close();
    Assertions.assertEquals(1, sourceCloses.get(), "segment close must close the chain sources exactly once");
    Assertions.assertEquals(0, segment.closes.get(), "unmanaged segment itself is not closed by the wrapper");
  }

  @Test
  void testCancelBeforeReadyClosesChain()
  {
    final TestSegment segment = new TestSegment();
    final SettableAsyncResource<String> source = new SettableAsyncResource<>();

    final AdaptedLoadableSegment adapted =
        new AdaptedLoadableSegment(() -> makeChain(segment, List.of(source)), DESCRIPTOR, "test", null);
    final AcquireSegmentAction action = adapted.acquire(AcquireMode.FULL);
    Assertions.assertFalse(action.isReady());

    // closing the un-released handle cancels: the chain (and its sources) are closed
    action.close();

    // a source completing after the cancel loses the set() race: the producer retains ownership
    final AtomicInteger lateCloses = new AtomicInteger();
    Assertions.assertFalse(source.set("late", lateCloses::incrementAndGet));
  }

  @Test
  void testChainFailureSurfacesAndClosesSources()
  {
    final TestSegment segment = new TestSegment();
    final AtomicInteger okSourceCloses = new AtomicInteger();
    final SettableAsyncResource<String> okSource = new SettableAsyncResource<>();
    okSource.set("ok", okSourceCloses::incrementAndGet);
    final SettableAsyncResource<String> failedSource = new SettableAsyncResource<>();

    final AdaptedLoadableSegment adapted = new AdaptedLoadableSegment(
        () -> makeChain(segment, List.of(okSource, failedSource)),
        DESCRIPTOR,
        "test",
        null
    );
    final AcquireSegmentAction action = adapted.acquire(AcquireMode.FULL);
    failedSource.setException(new IllegalStateException("fetch failed"));

    Assertions.assertTrue(action.isReady());
    Assertions.assertThrows(IllegalStateException.class, action::release);
    Assertions.assertEquals(1, okSourceCloses.get(), "chain failure must release the successful sources");
    action.close();
    Assertions.assertEquals(1, okSourceCloses.get(), "close after failure must not double-close sources");
  }

  @Test
  void testCloseAfterDeliveryWithoutReleaseClosesEverything()
  {
    final TestSegment segment = new TestSegment();
    final AtomicInteger sourceCloses = new AtomicInteger();
    final SettableAsyncResource<String> source = new SettableAsyncResource<>();
    source.set("file", sourceCloses::incrementAndGet);

    final AdaptedLoadableSegment adapted =
        new AdaptedLoadableSegment(() -> makeChain(segment, List.of(source)), DESCRIPTOR, "test", null);
    final AcquireSegmentAction action = adapted.acquire(AcquireMode.FULL);
    Assertions.assertTrue(action.isReady());

    // never released: closing the handle closes the delivered result, whose segment close tears down the chain
    action.close();
    Assertions.assertEquals(1, sourceCloses.get());
  }

  @Test
  void testFromUnmanagedSegmentDeliversNoopCloseWrapper() throws Exception
  {
    final TestSegment segment = new TestSegment();
    final AdaptedLoadableSegment adapted =
        AdaptedLoadableSegment.fromUnmanagedSegment(segment, DESCRIPTOR, "test", null);

    final AcquireSegmentAction action = adapted.acquire(AcquireMode.FULL);
    action.await();
    final AcquireSegmentResult result = action.release();
    final Segment delivered = result.getSegment().orElseThrow();
    delivered.close();
    Assertions.assertEquals(0, segment.closes.get(), "unmanaged segment must not be closed by the wrapper");
    action.close();
  }

  @Test
  void testAcquireTwiceThrows()
  {
    final AdaptedLoadableSegment adapted =
        AdaptedLoadableSegment.fromUnmanagedSegment(new TestSegment(), DESCRIPTOR, "test", null);
    adapted.acquire(AcquireMode.FULL).close();
    Assertions.assertThrows(Exception.class, () -> adapted.acquire(AcquireMode.FULL));
  }
}
