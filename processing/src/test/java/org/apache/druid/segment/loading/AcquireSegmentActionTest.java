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

package org.apache.druid.segment.loading;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestSegmentUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class AcquireSegmentActionTest
{
  private static Segment makeSegment()
  {
    return new TestSegmentUtils.SegmentForTesting("test", Intervals.of("2020-01-01/2020-01-02"), "v1");
  }

  private static class CloseCountingSegment extends TestSegmentUtils.SegmentForTesting
  {
    private final AtomicInteger closeCount = new AtomicInteger();

    CloseCountingSegment()
    {
      super("test", Intervals.of("2020-01-01/2020-01-02"), "v1");
    }

    @Override
    public void close()
    {
      closeCount.incrementAndGet();
      super.close();
    }
  }

  @Test
  public void testCompletedReleaseThenCloseIsNoop()
  {
    final CloseCountingSegment segment = new CloseCountingSegment();
    final AcquireSegmentAction action = AcquireSegmentAction.completed(
        AcquireSegmentResult.of(Optional.of(segment))
    );
    Assertions.assertTrue(action.isReady());
    final AcquireSegmentResult released = action.release();
    Assertions.assertSame(segment, released.getSegment().orElseThrow());
    // close after release is a no-op; the caller owns the released result
    action.close();
    Assertions.assertEquals(0, segment.closeCount.get());
    released.close();
    Assertions.assertEquals(1, segment.closeCount.get());
  }

  @Test
  public void testCloseWithoutReleaseClosesDeliveredResult()
  {
    final CloseCountingSegment segment = new CloseCountingSegment();
    final AcquireSegmentAction action = AcquireSegmentAction.completed(
        AcquireSegmentResult.of(Optional.of(segment))
    );
    action.close();
    Assertions.assertEquals(1, segment.closeCount.get());
  }

  @Test
  public void testReleaseAfterCloseThrows()
  {
    final AcquireSegmentAction action = AcquireSegmentAction.completed(
        AcquireSegmentResult.of(Optional.of(makeSegment()))
    );
    action.close();
    Assertions.assertThrows(DruidException.class, action::release);
  }

  @Test
  public void testDoubleCloseThrows()
  {
    final AcquireSegmentAction action = AcquireSegmentAction.missingSegment();
    action.close();
    Assertions.assertThrows(DruidException.class, action::close);
  }

  @Test
  public void testReleaseBeforeReadyThrows()
  {
    final AcquireSegmentAction action = new AcquireSegmentAction();
    Assertions.assertThrows(DruidException.class, action::release);
    action.close();
  }

  @Test
  public void testCloseBeforeReadyRunsCancelerAndOrphanedSetReturnsFalse()
  {
    final AtomicInteger cancels = new AtomicInteger();
    final AcquireSegmentAction action = new AcquireSegmentAction(cancels::incrementAndGet);
    action.close();
    Assertions.assertEquals(1, cancels.get());

    // producer loses the delivery race and retains ownership of the orphaned result
    final CloseCountingSegment segment = new CloseCountingSegment();
    final AcquireSegmentResult result = AcquireSegmentResult.of(Optional.of(segment));
    Assertions.assertFalse(action.set(result));
    Assertions.assertEquals(0, segment.closeCount.get());
    result.close();
    Assertions.assertEquals(1, segment.closeCount.get());
  }

  @Test
  public void testSetExceptionSurfacesFromReleaseAndGet()
  {
    final AcquireSegmentAction action = new AcquireSegmentAction();
    action.setException(new IllegalStateException("boom"));
    Assertions.assertTrue(action.isReady());
    Assertions.assertThrows(IllegalStateException.class, action::get);
    Assertions.assertThrows(IllegalStateException.class, action::release);
    action.close();
  }

  @Test
  public void testMissingSegmentDeliversEmptyResult()
  {
    final AcquireSegmentAction action = AcquireSegmentAction.missingSegment();
    Assertions.assertTrue(action.isReady());
    final AcquireSegmentResult result = action.release();
    Assertions.assertTrue(result.getSegment().isEmpty());
    Assertions.assertEquals(0, result.getLoadSizeBytes());
    // closing an empty result is a no-op
    result.close();
    result.close();
    action.close();
  }

  @Test
  public void testResultCloseIsIdempotent()
  {
    final CloseCountingSegment segment = new CloseCountingSegment();
    final AcquireSegmentResult result = new AcquireSegmentResult(Optional.of(segment), 1L, 2L, 3L);
    Assertions.assertEquals(1L, result.getLoadSizeBytes());
    Assertions.assertEquals(2L, result.getWaitTimeNanos());
    Assertions.assertEquals(3L, result.getLoadTimeNanos());
    result.close();
    result.close();
    Assertions.assertEquals(1, segment.closeCount.get());
  }

  @Test
  public void testSetCancelerAfterReadyIsNoop()
  {
    final AtomicInteger cancels = new AtomicInteger();
    final AcquireSegmentAction action = AcquireSegmentAction.completed(
        AcquireSegmentResult.of(Optional.of(makeSegment()))
    );
    action.setCanceler(cancels::incrementAndGet);
    action.close();
    Assertions.assertEquals(0, cancels.get());
  }
}
